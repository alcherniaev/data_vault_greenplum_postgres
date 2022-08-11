CREATE OR REPLACE FUNCTION rdv_2.load_transactions()
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
    declare
        v_begin_dtm timestamp;
        v_end_dtm timestamp;
        v_src_system text := 'PG';
        v_rc int;
    begin
    
        v_begin_dtm := (select coalesce(max(l.end_dtm), '1900-01-01'::timestamp) from rdv_2.t_loads l where l.table_name = 'hs_transactions' and l.src_system = v_src_system);
        v_end_dtm := now();
        
         raise notice 'Function rdv_2.load_transactions started with v_begin_dtm=% and v_end_dtm=%',v_begin_dtm,v_end_dtm;
                
        /*Очистка стейджа перед загрузкой*/
        truncate table stage.transactions_changes;
        
        /*Наполнени стейджинга из внешней таблицы, для дальнейшей обработки в ГП, без повторных обращений к источнику*/
        insert into stage.transactions_changes
             ( operation
             , stamp
	         , id
	         , client_id
	         , product_category
	         , product_company
	         , subtype
	         , amount
	         , "date"
	         , transaction_type
             )
        select operation
             , stamp
	         , id
	         , client_id
	         , product_category
	         , product_company
	         , subtype
	         , amount
	         , "date"
	         , transaction_type
          from pg.transactions_changes
         where stamp > v_begin_dtm
           and stamp <= v_end_dtm;

           get diagnostics v_rc = row_count;
         raise notice '% rows loaded to stage.transactions_changes',v_rc;
         
      truncate table stage.m_transactions;
      
        insert into stage.m_transactions
             ( transactions_sk
             , transactions_bk
             , src_system
             , src_change_dtm
             , src_action_type
             , row_hash
             , clients_sk
             , clients_bk
             , categories_sk
             , categories_bk
             , transactions_clients_sk
             , transactions_categories_sk
             , amount
             , transaction_dtm
             , type_sk
             , transaction_type
             , subtype
             , company_sk
             , company_name
             )
          with stg as (select sr.*
                            , row_number() over (partition by sr.transactions_sk order by sr.src_change_dtm) as rn
                         from ( select md5(s.id::text) as transactions_sk
                                     , s.id::text as transactions_bk
                                     , v_src_system as src_system
                                     , s.stamp as src_change_dtm
                                     , s.operation as src_action_type
                                     , md5(concat(s.amount, s."date", md5(transaction_type::text), md5(s.product_company::text), s.operation)) as row_hash
                                     , md5(s.client_id::text) as clients_sk
                                     , s.client_id::text as clients_bk
                                     , md5(s.product_category::text) as categories_sk
                                     , s.product_category::text as categories_bk
                                     , md5(concat(md5(s.id::text), md5(s.client_id::text))) as transactions_clients_sk
                                     , md5(concat(md5(s.id::text), md5(s.product_category::text))) as transactions_categories_sk
                                     , s.amount
                                     , s."date" as transaction_dtm
                                     , md5(concat(s.transaction_type, s.subtype)) as type_sk
                                     , s.transaction_type
                                     , s.subtype
                                     , md5(s.product_company::text) as company_sk
                                     , s.product_company as company_name
                                     , lag(md5(concat(s.amount, s."date", md5(transaction_type::text), md5(s.product_company::text), s.operation)),1,'0') over (partition by s.id order by s.stamp) as prev_row_hash
                                  from stage.transactions_changes s) sr
                        where row_hash <> prev_row_hash),
               last_hs as (select tr.transactions_sk
                                , tr.row_hash
                             from (select t.transactions_sk
                                        , t.row_hash
                                        , row_number() over(partition by t.transactions_sk order by src_change_dtm desc) as rn
                                     from rdv_2.hs_transactions t ) tr
                                    where tr.rn = 1)                    
        select s.transactions_sk
             , s.transactions_bk
             , s.src_system
             , s.src_change_dtm
             , s.src_action_type
             , s.row_hash
             , s.clients_sk
             , s.clients_bk
             , s.categories_sk
             , s.categories_bk
             , s.transactions_clients_sk
             , s.transactions_categories_sk
             , s.amount
             , s.transaction_dtm
             , s.type_sk
             , s.transaction_type
             , s.subtype
             , s.company_sk
             , s.company_name
          from stg s
          left join last_hs hs on hs.transactions_sk = s.transactions_sk
                               and hs.row_hash = s.row_hash
                               and s.rn = 1
         where hs.transactions_sk is null;
         
           get diagnostics v_rc = row_count;
         raise notice '% rows loaded to stage.m_transactions',v_rc;
         
           
        /*Наполнение хабов*/
        insert into rdv_2.h_transactions
             ( transactions_sk
             , transactions_bk
             , src_system
             , first_load_dtm
             )
        select s.transactions_sk
             , min(s.transactions_bk) transactions_bk
             , min(s.src_system) src_system
             , min(s.src_change_dtm) first_load_dtm
          from stage.m_transactions s
         where not exists (select t.transactions_sk from rdv_2.h_transactions t where t.transactions_sk = s.transactions_sk)
         group by s.transactions_sk;
         
           get diagnostics v_rc = row_count;
         raise notice '% rows loaded to rdv_2.h_transactions',v_rc;
        
        insert into rdv_2.h_clients
             ( clients_sk
             , clients_bk
             , src_system
             , first_load_dtm
             )
        select s.clients_sk
             , min(s.clients_bk) clients_bk
             , min(s.src_system) src_system
             , min(s.src_change_dtm) first_load_dtm
          from stage.m_transactions s
         where not exists (select t.clients_sk from rdv_2.h_clients t where t.clients_sk = s.clients_sk)
         group by s.clients_sk;
         
           get diagnostics v_rc = row_count;
         raise notice '% rows loaded to rdv_2.h_clients',v_rc;
         
        insert into rdv_2.h_categories
             ( categories_sk
             , categories_bk
             , src_system
             , first_load_dtm
             )
        select s.categories_sk
             , min(s.categories_bk) categories_bk
             , min(s.src_system) src_system
             , min(s.src_change_dtm) first_load_dtm
          from stage.m_transactions s
         where not exists (select t.categories_sk from rdv_2.h_categories t where t.categories_sk = s.categories_sk)
         group by s.categories_sk;
         
           get diagnostics v_rc = row_count;
         raise notice '% rows loaded to rdv_2.h_categories',v_rc;
         
         /*Наполнение ликнов*/
         insert into rdv_2.l_transactions_clients
              ( transactions_clients_sk
              , transactions_sk
              , clients_sk
              , src_system
              , first_load_dtm
              )
         select s.transactions_clients_sk
              , s.transactions_sk
              , s.clients_sk
              , s.src_system
              , s.src_change_dtm as first_load_dtm
           from stage.m_transactions s
          where not exists (select lt.transactions_clients_sk from rdv_2.l_transactions_clients lt where lt.transactions_clients_sk = s.transactions_clients_sk);
          
           get diagnostics v_rc = row_count;
         raise notice '% rows loaded to rdv_2.l_transactions_clients',v_rc;
         
         insert into rdv_2.l_transactions_categories
              ( transactions_categories_sk
              , transactions_sk
              , categories_sk
              , src_system
              , first_load_dtm
              )
         select s.transactions_categories_sk
              , s.transactions_sk
              , s.categories_sk
              , s.src_system
              , s.src_change_dtm as first_load_dtm
           from stage.m_transactions s
          where not exists (select lt.transactions_categories_sk from rdv_2.l_transactions_categories lt where lt.transactions_categories_sk = s.transactions_categories_sk);
          
           get diagnostics v_rc = row_count;
         raise notice '% rows loaded to rdv_2.l_transactions_categories',v_rc;
           
         /*Наполнение референсов*/
         insert into rdv_2.r_companies
              (	company_sk
              , company_name
              , src_system
              , src_change_dtm
              )
         select s.company_sk
              , min(s.company_name) company_name
              , min(s.src_system) src_system
              , min(s.src_change_dtm) src_change_dtm
           from stage.m_transactions s
          where not exists (select t.company_sk from rdv_2.r_companies t where t.company_sk = s.company_sk)
          group by s.company_sk; 
          
           get diagnostics v_rc = row_count;
         raise notice '% rows loaded to rdv_2.r_companies',v_rc;
         
         insert into rdv_2.r_transaction_types
              (	type_sk
              , subtype
              , transaction_type
              , src_system
              , src_change_dtm
              )
         select s.type_sk
              , min(s.subtype) subtype
              , min(s.transaction_type) transaction_type
              , min(s.src_system) src_system
              , min(s.src_change_dtm) src_change_dtm
          from stage.m_transactions s
          where not exists (select t.type_sk from rdv_2.r_transaction_types t where t.type_sk = s.type_sk)
          group by type_sk;   

           get diagnostics v_rc = row_count;
         raise notice '% rows loaded to rdv_2.r_transaction_types',v_rc;          
         
         /*Наполнение сателлитов*/
         insert into rdv_2.hs_transactions
              ( transactions_sk
              , src_system
              , src_change_dtm
              , src_action_type
              , row_hash
              , amount
              , transaction_dtm
              , type_sk
              , company_sk
              )
         select s.transactions_sk
              , s.src_system
              , s.src_change_dtm
              , s.src_action_type
              , s.row_hash
              , s.amount
              , s.transaction_dtm
              , s.type_sk
              , s.company_sk
           from stage.m_transactions s;
                                           
           get diagnostics v_rc = row_count;
         raise notice '% rows loaded to rdv_2.hs_transactions',v_rc;
                                           
         insert into rdv_2.ls_transactions_clients
              ( transactions_clients_sk
              , src_system
              , src_change_dtm
              , src_action_type
              , row_hash
              )
         select s.transactions_clients_sk
              , s.src_system
              , s.src_change_dtm
              , s.src_action_type
              , s.row_hash
           from stage.m_transactions s;
                                             
           get diagnostics v_rc = row_count;
         raise notice '% rows loaded to rdv_2.ls_transactions_clients',v_rc;
         
         insert into rdv_2.ls_transactions_categories
              ( transactions_categories_sk
              , src_system
              , src_change_dtm
              , src_action_type
              , row_hash
              )
         select s.transactions_categories_sk
              , s.src_system
              , s.src_change_dtm
              , s.src_action_type
              , s.row_hash
           from stage.m_transactions s;
                                             
           get diagnostics v_rc = row_count;           
         raise notice '% rows inserted into rdv_2.ls_transactions_categories',v_rc;
          
        insert into rdv_2.t_loads
        values ('hs_transactions', v_src_system, v_begin_dtm, v_end_dtm);
        
        raise notice 'Function rdv_2.load_transactions finished successful';
        
        return true;
    end

$$
EXECUTE ON ANY;
