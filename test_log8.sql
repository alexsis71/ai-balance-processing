/*
26.05.2025	2010000315491	Справочно:Тов. газ(без оседания)	меняет позицию, уровень, родителя, название	"name=Товарный газ (без оседания)
ord=24
lvl=2
parent=ID"
*/

 SELECT * FROM balance_api.fn_balance_article_rename
 (
  p_article_id =>2010000315491,
  p_article_name => 'Товарный газ (без оседания)',
  p_old_date => '26.05.2025',
  p_new_valid_date => '2099-12-31'
  );
 
 
SELECT FROM balance_api.fn_balance_article_level_set
 (
  p_article_id => 2010000315491,
  p_begin_date => '26.05.2025' ,
  p_end_date   => '2099-12-31',
  p_parent_id  => 2010000319835 ,
  p_level      => 2
  );
  
  
select from balance_api.fn_balance_article_ord_set
 (
  p_article_id => 2010000315491,
  p_article_ord =>24,
  p_valid_date => '26.05.2025'
 );
  