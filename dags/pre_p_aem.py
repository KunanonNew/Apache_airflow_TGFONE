import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import csv

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'pre_p_aem',
    description='A simple DAG to select data from PostgreSQL',
    schedule_interval=None,
    tags=["demo"],
):

    def select_data():
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        logging.info("Selecting data from PostgreSQL...")
        # Your data selection code here
        logging.info("Data selection completed.")

        sql = """
            select distinct 
		(case when i3.complete_name like '%/%' then split_part(i3.complete_name,'/',2)::text else i3.complete_name end) as cat1,
		(case when i3.complete_name like '%/%' then split_part(i3.complete_name,'/',3)::text else i3.complete_name end) as cat2,
		(case when i3.complete_name like '%/%' then split_part(i3.complete_name,'/',4)::text else i3.complete_name end) as cat3,
		(case when i3.complete_name like '%/%' then split_part(i3.complete_name,'/',5)::text else i3.complete_name end) as cat4,
		(case when i3.complete_name like '%/%' then split_part(i3.complete_name,'/',6)::text else i3.complete_name end) as cat5,
		a.product_id,f.default_code,f.product_name,i1.name as product_model,i.type as product_type,i2.name as Type_of_channels,h.name as color,j.name as Brand,i.active as active,i4.name AS Unit,i.list_price as SRP,f1.cu_product_status_id as status,f1.cu_product_group_id as product_group,
		a.location_id,c.complete_name as branch,b.complete_name as location_branch,d.name as branch_name,b.active as location_activate,
		d7.name as branch_shop,d8.name as branch_class,d1.name as branch_business,d2.name as branch_grade,d4.name as branch_type,d3.name as branch_zone,d6.name as branch_city,d5.name as Area_manager,sum(a.quantity) as QTY,sum(a.reserved_quantity) as reserved_qty,sum(a.available_quantity) as available_qty,
		cast(current_date as date) as date,to_char(current_date,'Month') as month,to_char(current_date,'YYYY') as Year
	from stock_quant a
left join stock_location b on a.location_id = b.id
left join stock_location c on b.location_id = c.id 
left join res_company_branches d on c.company_branch_id = d.id
left join res_company_branch_group e on d.company_branch_group_id = e.id
left join product_product f on a.product_id = f.id
left join product_template_attribute_value g on f.combination_indices = g.id::text
left join product_attribute_value h on g.product_attribute_value_id  = h.id
left join product_template i on f.product_tmpl_id = i.id
left join product_brand j on i.cu_product_brand_id = j.id
left join res_company_branch_business d1 on d.company_branch_business_id = d1.id
left join res_company_branch_grade d2  on d.company_branch_grade_id = d2.id
left join res_company_branch_type d4 on d.company_branch_type_id = d4.id
left join res_company_branch_zone d3 on d.company_branch_zone_id = d3.id
left join res_partner d5 on d.area_manager_id = d5.id
left join res_company_branch_route d6 on d.branch_location_route_id = d6.id
left join res_company_branch_location d7 on d.branch_location_id = d7.id 
left join res_company_branch_class d8 on d.company_branch_class_id = d8.id
left join cu_product_model i1 on i.cu_product_model_id = i1.id
left join cu_type_channels i2 on i.type_of_channels_id = i2.id
left join product_category i3 on i.categ_id = i3.id
left join uom_uom i4 on i.uom_id = i4.id
left join approval_request f1 on f.default_code = f1.barcode
where i.type = 'product' and (case when i3.complete_name like '%/%' then split_part(i3.complete_name,'/',2)::text else i3.complete_name end) like ('%SALE%')
group by a.product_id,c.complete_name,d.name,e.name,f.default_code,h.name,b.complete_name,f.product_name,a.location_id,j.name,b.active,d1.name,d2.name,d3.name,d4.name,d5.name,d6.name,d7.name,d8.name,i1.name,i.type,i2.name,i.active,i3.complete_name,i4.name,i.list_price,f1.cu_product_group_id,f1.cu_product_status_id
"""
        cursor.execute(sql)
        results = cursor.fetchall()

        cursor.close()
        connection.close()
        
    start_task = DummyOperator(task_id='start_task')
    end_task = DummyOperator(task_id='end_task')

    select_data_task = PythonOperator(
        task_id='select_data_task',
        python_callable=select_data,
)

start_task >> select_data_task >> end_task
