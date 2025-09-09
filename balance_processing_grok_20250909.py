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
            logging.error(f"Ошибка чтения файла: {e}")
            messagebox.showerror("Ошибка файла", f"Ошибка чтения Excel файла: {str(e)}")
            return None

    def process_file(self, file_path: str, generate_file: bool = False) -> List[str]:
        """Основная обработка файла"""
        df = self.read_excel_file(file_path)
        if df is None:
            return []
            
        sql_queries = []
        
        if not self.connect_db():
            return []
        
        try:
            if generate_file:
                sql_queries.append("-- ===========================================")
                sql_queries.append(f"-- SQL скрипт сгенерирован автоматически")
                sql_queries.append(f"-- Дата генерации: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                sql_queries.append(f"-- Источник: {os.path.basename(file_path)}")
                sql_queries.append(f"-- Report ID: {self.report_id}")
                sql_queries.append("-- ===========================================\n")
            
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
                if pd.notna(action_col) and 'добавление статьи' in str(action_col).lower() and pd.notna(attr_value):
                    attrs = self.parse_attributes(attr_value)
                    if 'parent' in attrs:
                        self.get_or_generate_id(attrs['parent'])
                    
                    # Если в колонке ID статьи пусто - создаем временный ID
                    if pd.isna(id_col) or not str(id_col).strip():
                        temp_id = f"TEMP_{temp_id_counter}"
                        temp_id_counter += 1
                        # Сохраняем временный ID в строке для использования во втором проходе
                        df.at[index, 'id статьи'] = temp_id
                        self.get_or_generate_id(temp_id)
            
            # Второй проход: генерируем SQL
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
                    
                    change_date_str = change_date.strftime('%Y-%m-%d')
                    
                    id_col = row['id статьи']
                    name_col = row['имя статьи'] if 'имя статьи' in row else pd.NaT
                    action_col = row['действие'] if 'действие' in row else pd.NaT
                    attr_value = row['значение атрибута'] if 'значение атрибута' in row else pd.NaT
                    
                    if generate_file:
                        sql_queries.append(f"-- Строка {index + 3}: {action_col}")
                        
                    # Обработка РЕНУМЕРАЦИИ
                    renum_action = self.parse_renum_action(id_col)
                    if renum_action:
                        begin_ord, end_ord, shift_ord = renum_action
                        if generate_file:
                            sql_queries.append(f"-- Строка {index + 3}: Ренумерация - {id_col}")
                        
                        sql = f"""SELECT balance_api.fn_balance_article_renum_up_down(
    p_report_id => {self.report_id},
    p_begin_ord => {begin_ord},
    p_end_ord => {end_ord},
    p_shift_ord => {shift_ord},
    p_old_valid_date => '{change_date_str}',
    p_new_valid_date => '{DEFAULT_NEW_VALID_DATE}'
);"""
                        sql_queries.append(sql)
                        continue
                    
                    # Обработка других действий
                    if pd.notna(action_col):
                        action_text = str(action_col).strip().lower()
                        
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
                                    sql_queries.append(sql)
                        
                        # Добавление статьи
                        elif 'добавление статьи' in action_text:
                            if pd.notna(attr_value):
                                attrs = self.parse_attributes(attr_value)
                                if all(k in attrs for k in ['name', 'ord', 'lvl', 'parent']):
                                    # Берем article_id из колонки "ID статьи" (уже сгенерирован в первом проходе)
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
                                        sql_queries.append(sql)
                        
                        # Изменение уровня и родителя
                        elif 'меняет уровень и родителя' in action_text:
                            if pd.notna(attr_value):
                                attrs = self.parse_attributes(attr_value)
                                article_id = self.get_or_generate_id(id_col)
                                
                                if article_id and 'lvl' in attrs and 'parent' in attrs:
                                    # Ищем сгенерированный ID для родителя
                                    parent_temp_id = attrs['parent'].strip()
                                    parent_id = self.get_or_generate_id(parent_temp_id)
                                    
                                    level = attrs['lvl']
                                    
                                    if parent_id:
                                        sql = f"""SELECT balance_api.fn_balance_article_level_set(
    p_article_id => {article_id},
    p_begin_date => '{change_date_str}',
    p_end_date => '{DEFAULT_NEW_VALID_DATE}',
    p_parent_id => {parent_id},
    p_level => {level}
);"""
                                        sql_queries.append(sql)
                                    else:
                                        logging.warning(f"Не удалось найти ID для родителя '{parent_temp_id}' в строке {index + 3}")
                        else:
                            if generate_file:
                                sql_queries.append("-- Неизвестный тип действия")
                    else:
                        continue
                                    
                except Exception as e:
                    error_msg = f"-- ОШИБКА в строке {index + 3}: {str(e)}"
                    if generate_file:
                        sql_queries.append(error_msg)
                    logging.error(error_msg)
                    continue
            
            if generate_file:
                sql_queries.append("\n-- ===========================================")
                sql_queries.append("-- Конец скрипта")
                sql_queries.append("-- ===========================================")
                    
        except Exception as e:
            logging.error(f"Ошибка обработки: {e}")
            messagebox.showerror("Ошибка обработки", f"Ошибка при обработке файла: {str(e)}")
        finally:
            if not generate_file:
                self.disconnect_db()
                
        return sql_queries

    def execute_queries(self, queries: List[str]) -> bool:
        """Выполнение SQL запросов в БД"""
        if not self.connect_db():
            return False
            
        try:
            with self.conn.cursor() as cursor:
                for i, query in enumerate(queries, 1):
                    if query.strip().startswith('--') or not query.strip():
                        continue
                    logging.info(f"Выполнение запроса {i}/{len(queries)}")
                    cursor.execute(query)
            self.conn.commit()
            logging.info("Все запросы выполнены успешно")
            return True
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Ошибка выполнения запросов: {e}")
            messagebox.showerror("Ошибка выполнения", f"Ошибка при выполнении SQL запросов: {str(e)}")
            return False
        finally:
            self.disconnect_db()

    def save_sql_file(self, queries: List[str], output_path: str) -> bool:
        """Сохранение SQL запросов в файл"""
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                for query in queries:
                    f.write(query + '\n')
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

def select_file():
    root = tk.Tk()
    root.withdraw()
    return filedialog.askopenfilename(
        title="Выберите файл с изменениями баланса",
        filetypes=[("Excel files", "*.xlsx"), ("All files", "*.*")]
    )

def select_save_file():
    root = tk.Tk()
    root.withdraw()
    return filedialog.asksaveasfilename(
        title="Сохранить SQL файл",
        defaultextension=".sql",
        filetypes=[("SQL files", "*.sql"), ("All files", "*.*")]
    )

def ask_mode():
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
    input_file = select_file()
    if not input_file:
        print("Файл не выбран")
        return
    execute_in_db = ask_mode()
    processor = BalanceProcessor(config)
    
    if execute_in_db:
        queries = processor.process_file(input_file, generate_file=False)
        if queries:
            success = processor.execute_queries(queries)
            if success:
                messagebox.showinfo("Успех", "Запросы успешно выполнены в БД")
                print("✅ Запросы успешно выполнены в БД")
            else:
                print("❌ Ошибка при выполнении запросов")
        else:
            messagebox.showinfo("Информация", "Не найдено изменений для обработки")
            print("Не найдено изменений для обработки")
    else:
        output_file = select_save_file()
        if not output_file:
            print("Файл для сохранения не выбран")
            return
        queries = processor.process_file(input_file, generate_file=True)
        if queries and len(queries) > 10:
            success = processor.save_sql_file(queries, output_file)
            if success:
                sql_count = len([q for q in queries if q.strip() and not q.startswith('--')])
                messagebox.showinfo("Успех", 
                                  f"SQL файл успешно сгенерирован!\n\n"
                                  f"Файл: {output_file}\n"
                                  f"Количество запросов: {sql_count}")
                print(f"✅ SQL файл успешно сгенерирован!")
                print(f"📁 Файл: {output_file}")
                print(f"📊 Количество SQL запросов: {sql_count}")
            else:
                print("❌ Ошибка при сохранении файла")
        else:
            messagebox.showinfo("Информация", "Не найдено изменений для обработки")
            print("Не найдено изменений для обработки")

if __name__ == "__main__":
    main()