create table stage.m_hs_transactions (
transactions_sk text not null
 , transactions_bk text not null
 , src_system text not null
 , src_change_dtm timestamp not null
 , src_action_type char(1) not null
 , row_hash text not null
 , amount int
 , transaction_dtm timestamp not null
 , type_sk text
 , company_sk text
)
distributedd by (transactions_sk);

CREATE OR REPLACE FUNCTION rdv_2.load_hs_transactions()
	RETURNS bool
	LANGUAGE plpgsql
	VOLATILE
AS $$
	
    declare
    	v_src_system text := 'PG';
    	v_rc int;
    begin
    	
	     /*Очистка стейджа перед загрузкой*/
      truncate table stage.categories_changes;
        
        /*Наполнение стейджинга из внешней таблицы, для дальнейшей обработки в ГП, без повторных обращений к источнику*/
        insert into stage.categories_changes
             ( operation
             , stamp
	         , id
	         , "name"
	         , description
	         , "mcc-code"
             )
        select operation
             , stamp
	         , id
	         , "name"
	         , description
	         , "mcc-code"
          from pg.categories_changes
         where stamp > v_begin_dtm
           and stamp <= v_end_dtm;
         
           get diagnostics v_rc = row_count;
         raise notice '% rows loaded to stage.categories_changes',v_rc;
	    
	    
	    
	    
	    /*наполняем декомпозированные таблицы*/
        truncate table stage.m_hs_transactions;
       
       		insert into stage.m_hs_transactions(
       		   transactions_sk 
			 ,transactions_bk 
			 ,src_system 
			 ,src_change_dtm 
			 ,src_action_type 
			 ,row_hash 
			 ,amount
			 ,transaction_dtm 
			 ,type_sk
			 ,company_sk
 			)
 			with stg as (select sr.*
 							, row_number over (partition by sr.transactions_sk order by sr.src_change_dtm) as rn
 						from ( select 
 									 md5(s.id::text) as transactions_sk
 									,v_src_system as src_system
 									,s.stamp as src_change_dtm
 									,s.operator as src_action_type
 									,md5(concat(s.amount, s."date", md5(transaction_type::text), md5(s.product_company::text), s.operator)) as row_hash
 									,s.amount as amount
 									,s."date" as transaction_dtm
 									,md5(s.product_company::text) as company_sk
									,lag(md5(concat(s.amount, s."date", md5(transaction_type::text), md5(s.product_company::text), s.operator)), 1, '0') over (partition by s.id order by s.stamp) prev_row_hash
								from stage.transations_changes s) sr
						where row_hash <> prev_row_hash),
				last_hs as (select tr.transactions_sk
									,t.row_hash
									,row_number() over (partition by t.transactions_sk order by src_change_dtm desc) as rn
							from rdv_2.hs_transactions t ) tr
							where tr.rn = 1)


			select s.transactions_sk
					,s.transactions_bk
					,s.src_system
					,s.src_change_dtm
					,s.src_action_type
					,s.row_hash
					,s.amount
					,s.transaction_dtm
					,s.type_sk
					,s.company_sk
			from stg s
			left join last_hs hs on hs.transactions_sk = s.transactions_sk
						and hs.row_hash = s.row_hash
						and s.rn = 1
			where hs.transactions_sk is null;
				
				get diagnostics v_rc = row_count;
			raise notice '% rows loaded to stage.m_hs_transactions', v_rc; 	

			/*наполняем хабы*/

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
          from stage.m_hs_transactions s
		  where not exists (select t.transactions_sk from rdv_2.h_transactions t where t.transactions_sk = s.transactions_sk)
          group by s.transactions_sk;
 							 
 							
 			/*наполняем сателлиты*/
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
           from stage.m_hs_transactions s;
                                           
           get diagnostics v_rc = row_count;
         raise notice '% rows loaded to rdv_2.hs_transactions',v_rc;

		 raise notice '% function rdv_2.load_hs_transactions finished successful',v_rc;		
		 return true;		
 							
 	end
    $$;