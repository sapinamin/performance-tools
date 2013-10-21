use ${DB};

-- following settings are required to generate appropriate number of mappers 
-- for ORC scale factor 200: 
set  mapreduce.input.fileinputformat.split.minsize=200000000;  
set  mapreduce.input.fileinputformat.split.maxsize=200000000;  
-- for ORC scale factor 1000:
-- set  mapreduce.input.fileinputformat.split.maxsize=1000089600;
-- set  mapreduce.input.fileinputformat.split.minsize=1000089600;

set hive.use.tez.natively=true;                              
set hive.enable.mrr=true; 
set hive.vectorized.execution.enabled=true;

select ss_item_sk from store_sales where ss_item_sk > 1 and ss_item_sk < 1000 limit 10;
