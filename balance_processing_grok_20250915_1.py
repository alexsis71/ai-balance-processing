import os
import re
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Tuple, Any

import pandas as pd
import tkinter as tk
from tkinter import filedialog, messagebox
import psycopg2
from dotenv import load_dotenv

# Константы
APP_TITLE = "Balance Processor"
DEFAULT_NEW_VALID_DATE = "2099-12-31"

class BalanceProcessor:
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.generated_ids: Dict[str, int] = {}
        self.report_id = None
        self.conn = None
        
        # Настройка логирования
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('balance_processor.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )

    def connect_db(self):
        """Установка соединения с PostgreSQL"""
        try:
            logging.info(f"Попытка подключения к БД с параметрами: { {k: v for k, v in self.db_config.items() if k != 'password'} }")
            self.conn = psycopg2.connect(
                host=self.db_config['host'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                port=self.db_config['port'],
                connect_timeout=10
            )
            self.conn.autocommit = False
            logging.info("Подключение к БД установлено")
            return True
        except psycopg2.OperationalError as e:
            logging.error(f"Ошибка подключения к БД (OperationalError): {e}")
            messagebox.showerror("Ошибка подключения", 
                               f"Не удалось подключиться к БД:\n{str(e)}\n\n"
                               f"Проверьте параметры в .env файле:\n"
                               f"Хост: {self.db_config['host']}\n"
                               f"База: {self.db_config['database']}\n"
                               f"Пользователь: {self.db_config['user']}\n"
                               f"Порт: {self.db_config['port']}")
            return False
        except psycopg2.Error as e:
            logging.error(f"Ошибка PostgreSQL: {e}")
            messagebox.showerror("Ошибка БД", f"Ошибка PostgreSQL: {str(e)}")
            return False
        except Exception as e:
            logging.error(f"Неизвестная ошибка подключения: {e}")
            messagebox.showerror("Ошибка", f"Неизвестная ошибка: {str(e)}")
            return False

    def disconnect_db(self):
        """Закрытие соединения с БД"""
        if self.conn:
            try:
                self.conn.close()
                logging.info("Подключение к БД закрыто")
            except Exception as e:
                logging.error(f"Ошибка при закрытии подключения: {e}")
            finally:
                self.conn = None

    def get_or_generate_id(self, temp_id: str) -> Optional[int]:
        """Получение или генерация ID для временного идентификатора через БД"""
        if not temp_id or pd.isna(temp_id):
            return None
            
        temp_id = str(temp_id).strip()
        
        # Если это уже число
        if temp_id.isdigit():
            return int(temp_id)
            
        # Если это временный ID (ID, ID 1, ID 2...)
        if temp_id.upper().startswith(('ID', 'TEMP')):
            if temp_id in self.generated_ids:
                return self.generated_ids[temp_id]
                
            # Генерируем новый ID через функцию БД
            try:
                with self.conn.cursor() as cursor:
                    cursor.execute("SELECT balance_api.fn_get_new_obj_ids(1)")
                    new_id = cursor.fetchone()[0]
                    self.generated_ids[temp_id] = new_id
                    logging.info(f"Сгенерирован ID для '{temp_id}': {new_id}")
                    return new_id
            except Exception as e:
                logging.error(f"Ошибка генерации ID для {temp_id}: {e}")
                return None
                
        return None

    def parse_renum_action(self, id_text: str) -> Optional[Tuple[int, int, int]]:
        """Парсинг текста ренумерации из колонки ID статьи"""
        if not id_text or pd.isna(id_text):
            return None
            
        text = str(id_text).lower().strip()
        
        patterns = [
            r'статьи с порядком\s*>\s*(\d+)\s+вниз на\s*\+?(\d+)',
            r'статьи с порядком\s*>=\s*(\d+)\s+вниз на\s*\+?(\d+)',
            r'статьи с порядком\s*>=\s*(\d+)\s+и\s+порядком\s*<=\s*(\d+)\s+вниз на\s*\+?(\d+)'
        ]
        
        for i, pattern in enumerate(patterns):
            match = re.search(pattern, text)
            if match:
                groups = match.groups()
                if len(groups) == 3:
                    return int(groups[0]), int(groups[1]), int(groups[2])
                elif len(groups) == 2:
                    condition_value = int(groups[0])
                    shift_ord = int(groups[1])
                    if i == 0:
                        begin_ord = condition_value + 1
                    else:
                        begin_ord = condition_value
                    end_ord = 1000
                    return begin_ord, end_ord, shift_ord
        return None

    def parse_attributes(self, attr_text: str) -> Dict[str, str]:
        """Парсинг атрибутов из значения"""
        attrs = {}
        if pd.notna(attr_text):
            lines = re.split(r'<br\s*/?>|\n', str(attr_text), flags=re.IGNORECASE)
            for line in lines:
                line = line.strip()
                if '=' in line:
                    key, value = line.split('=', 1)
                    attrs[key.strip().lower()] = value.strip()
        return attrs

    def read_excel_file(self, file_path: str) -> Optional[pd.DataFrame]:
        """Чтение и валидация Excel файла"""
        try:
            header_df = pd.read_excel(file_path, header=None, nrows=1)
            self.report_id = header_df.iloc[0, 1]
            if pd.isna(self.report_id):
                raise ValueError("Не найден report_id в ячейке B1")
                
            df = pd.read_excel(file_path, header=1)
            df.columns = [str(col).strip().lower() for col in df.columns]
            
            return df
            
        except Exception as e:
            logging.error(f"Ошибка чтения файла {file_path}: {e}")
            return None

    def process_file(self, file_path: str, generate_file: bool = False) -> List[str]:
        """Основная обработка файла"""
        # Сбрасываем generated_ids для каждого файла
        self.generated_ids = {}
        
        df = self.read_excel_file(file_path)
        if df is None:
            return []
            
        sql_queries = []
        
        if not self.connect_db():
            return []
        
        try:
            if generate_file:
                sql_queries.append(f"-- Источник: {os.path.basename(file_path)}")
                sql_queries.append(f"-- Report ID: {self.report_id}")
                sql_queries.append("/*")
            
            # Первый проход: генерируем ID для всех временных идентификаторов
            temp_id_counter = 1
            for index, row in df.iterrows():
                if pd.isna(row['дата изменения']):
                    continue
                    
                id_col = row['id статьи']
                action_col = row['действие'] if 'действие' in row else pd.NaT
                attr_value = row['значение атрибута'] if 'значение атрибута' in row else pd.NaT
                
                # Для статей с ID в колонке "ID статьи" - генерируем ID
                if pd.notna(id_col) and not self.parse_renum_action(id_col):
                    self.get_or_generate_id(id_col)
                
                # Для добавления статей - также генерируем ID для parent
                if pd.notna(attr_value):
                    attrs = self.parse_attributes(attr_value)
                    if 'parent' in attrs:
                        self.get_or_generate_id(attrs['parent'])
                    
                    # Если в колонке ID статьи пусто - создаем временный ID
                    if pd.isna(id_col) or not str(id_col).strip():
                        temp_id = f"TEMP_{temp_id_counter}"
                        temp_id_counter += 1
                        df.at[index, 'id статьи'] = temp_id
                        self.get_or_generate_id(temp_id)
            
            # Собираем комментарии для начального блока если generate_file
            comment_lines = []
            
            # Списки для разных типов SQL
            renum_sql = []
            addition_sql = []
            change_sql = []
            
            # Второй проход: генерируем SQL по типам
            previous_begin = None
            for index, row in df.iterrows():
                try:
                    if pd.isna(row['дата изменения']):
                        continue
                        
                    change_date = row['дата изменения']
                    # Конвертация Excel serial date в datetime, если числовой
                    if isinstance(change_date, (int, float)):
                        change_date = pd.to_datetime(change_date, unit='D', origin='1899-12-30')
                    elif isinstance(change_date, str):
                        change_date = pd.to_datetime(change_date)
                    
                    change_date_str = change_date.strftime('%d.%m.%Y')
                    
                    id_col = row['id статьи']
                    name_col = row['имя статьи'] if 'имя статьи' in row else ''
                    action_col = row['действие'] if 'действие' in row else ''
                    attr_value = row['значение атрибута'] if 'значение атрибута' in row else ''
                    
                    if generate_file:
                        comment_lines.append(f"{change_date_str}\t{id_col}\t{name_col}\t{action_col}\t\"{attr_value}\"")
                        
                    # Обработка РЕНУМЕРАЦИИ
                    renum_action = self.parse_renum_action(id_col)
                    if renum_action:
                        begin_ord, end_ord, shift_ord = renum_action
                        if end_ord == 1000 and previous_begin is not None:
                            end_ord = previous_begin - 1
                        
                        sql = f"""SELECT balance_api.fn_balance_article_renum_up_down(
    p_report_id => {self.report_id},
    p_begin_ord => {begin_ord},
    p_end_ord => {end_ord},
    p_shift_ord => {shift_ord},
    p_old_valid_date => '{change_date_str}',
    p_new_valid_date => '{DEFAULT_NEW_VALID_DATE}'
);"""
                        renum_sql.append(sql)
                        previous_begin = begin_ord
                        continue
                    
                    attrs = {}
                    if pd.notna(attr_value):
                        attrs = self.parse_attributes(attr_value)
                    
                    is_addition = False
                    if pd.notna(action_col) and 'добавление статьи' in str(action_col).lower():
                        is_addition = True
                    elif pd.isna(action_col) and all(k in attrs for k in ['name', 'ord', 'lvl', 'parent']):
                        is_addition = True
                    
                    # Обработка других действий
                    action_text = str(action_col).strip().lower() if pd.notna(action_col) else ''
                    if pd.notna(action_col):
                        
                        # Переименование
                        if 'сменила название на' in action_text:
                            if pd.notna(attr_value):
                                article_id = self.get_or_generate_id(id_col)
                                if article_id:
                                    sql = f"""SELECT balance_api.fn_balance_article_rename(
    p_article_id => {article_id},
    p_article_name => '{attr_value}',
    p_old_date => '{change_date_str}',
    p_new_valid_date => '{DEFAULT_NEW_VALID_DATE}'
);"""
                                    change_sql.append(sql)
                        
                        # Изменение уровня и родителя
                        elif 'меняет уровень и родителя' in action_text or 'меняет уровень, родителя' in action_text:
                            article_id = self.get_or_generate_id(id_col)
                                
                            if article_id:
                                # Ищем сгенерированный ID для родителя
                                parent_temp_id = attrs.get('parent', '').strip()
                                parent_id = self.get_or_generate_id(parent_temp_id) if parent_temp_id else None
                                    
                                lvl = attrs.get('lvl', '-1')
                                    
                                if parent_id is not None or lvl != '-1':
                                    sql = f"""SELECT balance_api.fn_balance_article_level_set(
    p_article_id => {article_id},
    p_begin_date => '{change_date_str}',
    p_end_date => '{DEFAULT_NEW_VALID_DATE}',
    p_parent_id => {parent_id if parent_id else 'NULL'},
    p_level => {lvl}
);"""
                                    change_sql.append(sql)
                                else:
                                    logging.warning(f"Не удалось найти параметры для level_set в строке {index + 3}")
                        
                        # Логическое удаление
                        elif 'логически удаляем из документа' in action_text:
                            article_id = self.get_or_generate_id(id_col)
                            if article_id:
                                sql = f"""SELECT balance_api.fn_balance_article_end_date_set(
    {article_id},
    '{change_date_str}',
    true
);"""
                                change_sql.append(sql)
                        
                        # Новое действие: меняет ...
                        elif 'меняет' in action_text:
                            article_id = self.get_or_generate_id(id_col)
                            if article_id:
                                # Парсинг parent из action если "(родитель=XXX остается)"
                                parent_from_action = None
                                match = re.search(r'\(родитель=(\d+) остается\)', action_text)
                                if match:
                                    parent_from_action = match.group(1)
                                
                                # Проверяем, что меняется
                                changed_items = [item.split(' (')[0].strip() for item in action_text.split('меняет ')[1].split(', ') if item.strip()]
                                
                                # Если меняет имя или название и есть name
                                if ('имя' in changed_items or 'название' in changed_items) and 'name' in attrs:
                                    sql = f"""SELECT * FROM balance_api.fn_balance_article_rename(
    p_article_id => {article_id},
    p_article_name => '{attrs['name']}',
    p_old_date => '{change_date_str}',
    p_new_valid_date => '{DEFAULT_NEW_VALID_DATE}'
);"""
                                    change_sql.append(sql)
                                
                                # Если меняет ord или позицию и есть ord
                                if ('ord' in changed_items or 'позицию' in changed_items) and 'ord' in attrs:
                                    sql = f"""select from balance_api.fn_balance_article_ord_set(
    p_article_id =>{article_id} ,
    p_article_ord =>{attrs['ord']},
    p_valid_date =>'{change_date_str}'
);"""
                                    change_sql.append(sql)
                                
                                # Если меняет уровень или родителя
                                if 'уровень' in changed_items or 'родителя' in changed_items:
                                    lvl = attrs.get('lvl', '-1')  # default -1 если нет
                                    parent_temp = attrs.get('parent')
                                    if not parent_temp and parent_from_action:
                                        parent_temp = parent_from_action
                                    parent_id = self.get_or_generate_id(parent_temp) if parent_temp else None
                                    if parent_id is not None or lvl != '-1':
                                        sql = f"""select balance_api.fn_balance_article_level_set(
    p_article_id => {article_id},
    p_begin_date => '{change_date_str}',
    p_end_date   => '{DEFAULT_NEW_VALID_DATE}',
    p_parent_id  => {parent_id if parent_id else 'NULL'},
    p_level      => {lvl}
);"""
                                        change_sql.append(sql)
                                    else:
                                        logging.warning(f"Не найден parent или lvl для статьи {id_col} в строке {index + 3}")
                        
                        else:
                            if generate_file:
                                sql_queries.append("-- Неизвестный тип действия")
                    
                    # Добавление статьи
                    if is_addition:
                        article_id = self.get_or_generate_id(id_col)
                        parent_id = self.get_or_generate_id(attrs['parent'])
                        
                        if article_id:
                            parent_sql = f"{parent_id}" if parent_id else "NULL"
                            sql = f"""SELECT balance_api.fn_balance_article_add_1(
    p_report_id => {self.report_id},
    p_article_name => '{attrs['name']}',
    p_article_ord => {attrs['ord']},
    p_begin_date => '{change_date_str}',
    p_end_date => '{DEFAULT_NEW_VALID_DATE}',
    p_parent_id => {parent_sql},
    p_level => {attrs['lvl']},
    p_article_id => {article_id}
);"""
                            addition_sql.append(sql)
                    else:
                        continue
                                    
                except Exception as e:
                    error_msg = f"-- ОШИБКА в строке {index + 3}: {str(e)}"
                    if generate_file:
                        sql_queries.append(error_msg)
                    logging.error(error_msg)
                    continue
            
            # Собираем все SQL в порядок: ренумерации -> добавления -> изменения
            sql_queries.extend(renum_sql)
            sql_queries.extend(addition_sql)
            sql_queries.extend(change_sql)
            
            if generate_file:
                if comment_lines:
                    sql_queries.append("*/")
            
        except Exception as e:
            logging.error(f"Ошибка обработки файла {file_path}: {e}")
            if generate_file:
                sql_queries.append(f"-- ОШИБКА обработки файла {file_path}: {str(e)}")
        finally:
            if not generate_file:
                self.disconnect_db()
                
        return sql_queries

    def execute_queries(self, queries: List[str], file_path: str) -> bool:
        """Выполнение SQL запросов в БД для одного файла"""
        if not self.connect_db():
            return False
            
        try:
            with self.conn.cursor() as cursor:
                for i, query in enumerate(queries, 1):
                    if query.strip().startswith('--') or not query.strip():
                        continue
                    logging.info(f"Выполнение запроса {i}/{len(queries)} для файла {file_path}")
                    cursor.execute(query)
            self.conn.commit()
            logging.info(f"Все запросы для файла {file_path} выполнены успешно")
            return True
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Ошибка выполнения запросов для файла {file_path}: {e}")
            return False
        finally:
            self.disconnect_db()

    def save_sql_file(self, queries: List[List[str]], output_path: str) -> bool:
        """Сохранение SQL запросов для всех файлов в один файл"""
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write("-- ===========================================\n")
                f.write(f"-- SQL скрипт сгенерирован автоматически\n")
                f.write(f"-- Дата генерации: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("-- ===========================================\n\n")
                for file_queries in queries:
                    if not file_queries:
                        continue
                    for query in file_queries:
                        # Убедимся, что query - строка
                        f.write(str(query) + '\n')
                    f.write("\n")
                f.write("-- ===========================================\n")
                f.write("-- Конец скрипта\n")
                f.write("-- ===========================================\n")
            logging.info(f"SQL файл сохранен: {output_path}")
            return True
        except Exception as e:
            logging.error(f"Ошибка сохранения файла: {e}")
            messagebox.showerror("Ошибка сохранения", f"Ошибка при сохранении файла: {str(e)}")
            return False

def load_config():
    """Загрузка конфигурации БД"""
    try:
        load_dotenv()
        config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'port': os.getenv('DB_PORT', '5432')
        }
        if not config['database'] or not config['user'] or not config['password']:
            messagebox.showerror("Ошибка конфигурации", 
                               "Не заполнены обязательные параметры БД в .env файле:\n"
                               "DB_NAME, DB_USER, DB_PASSWORD")
            return None
        return config
    except Exception as e:
        messagebox.showerror("Ошибка конфигурации", f"Ошибка загрузки конфигурации: {str(e)}")
        return None

def select_files():
    """Выбор нескольких Excel файлов"""
    root = tk.Tk()
    root.withdraw()
    return filedialog.askopenfilenames(
        title="Выберите файлы с изменениями баланса",
        filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
    )

def select_save_file():
    """Выбор файла для сохранения SQL"""
    root = tk.Tk()
    root.withdraw()
    return filedialog.asksaveasfilename(
        title="Сохранить SQL файл",
        initialfile="output.sql",
        defaultextension=".sql",
        filetypes=[("SQL files", "*.sql"), ("All files", "*.*")]
    )

def ask_mode():
    """Запрос режима работы"""
    root = tk.Tk()
    root.withdraw()
    choice = messagebox.askquestion(
        APP_TITLE,
        "Выберите режим работы:\n\n" +
        "Да - выполнить запросы в БД\n" +
        "Нет - сгенерировать SQL файл",
        icon='question'
    )
    return choice == 'yes'

def main():
    print("=== Обработчик балансовых статей ===")
    config = load_config()
    if config is None:
        return
    input_files = select_files()
    if not input_files:
        print("Файлы не выбраны")
        return
    execute_in_db = ask_mode()
    processor = BalanceProcessor(config)
    
    if execute_in_db:
        success_count = 0
        for file_path in input_files:
            print(f"Обработка файла: {file_path}")
            queries = processor.process_file(file_path, generate_file=False)
            if queries:
                success = processor.execute_queries(queries, file_path)
                if success:
                    success_count += 1
                    print(f"✅ Файл {file_path} успешно обработан")
                else:
                    print(f"❌ Ошибка при обработке файла {file_path}")
            else:
                print(f"⚠️ Файл {file_path}: Не найдено изменений для обработки")
        messagebox.showinfo("Успех", f"Обработано файлов: {success_count} из {len(input_files)}")
        print(f"Обработано файлов: {success_count}/{len(input_files)}")
    else:
        output_file = select_save_file()
        if not output_file:
            print("Файл для сохранения не выбран")
            return
        all_queries = []
        success_count = 0
        for file_path in input_files:
            print(f"Обработка файла: {file_path}")
            queries = processor.process_file(file_path, generate_file=True)
            if queries:
                all_queries.append(queries)
                success_count += 1
                print(f"✅ Файл {file_path} успешно обработан")
            else:
                print(f"⚠️ Файл {file_path}: Не найдено изменений для обработки")
        if all_queries:
            success = processor.save_sql_file(all_queries, output_file)
            if success:
                sql_count = sum(len([q for q in file_queries if q.strip() and not q.startswith('--')]) for file_queries in all_queries)
                messagebox.showinfo("Успех", 
                                  f"SQL файл успешно сгенерирован!\n\n"
                                  f"Файл: {output_file}\n"
                                  f"Обработано файлов: {success_count}/{len(input_files)}\n"
                                  f"Количество запросов: {sql_count}")
                print(f"✅ SQL файл успешно сгенерирован!")
                print(f"📁 Файл: {output_file}")
                print(f"📊 Количество SQL запросов: {sql_count}")
            else:
                print("❌ Ошибка при сохранении файла")
        else:
            messagebox.showinfo("Информация", "Не найдено изменений для обработки во всех файлах")
            print("Не найдено изменений для обработки во всех файлах")

if __name__ == "__main__":
    main()