#airflow imports
from airflow import DAG
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy_operator import DummyOperator

#Couture imports
from CouturePythonDockerPlugin import CouturePythonDockerOperator
from CoutureSparkPlugin import CoutureSparkOperator
from CoutureSpark3Plugin import CoutureSpark3Operator

#Python imports
import re
import pprint
from datetime import datetime, timedelta

# python file imports
from search_dag_utils import change_vertical_name


pp = pprint.PrettyPrinter(indent=4)

# ============================== #
# ======= Data Variables ======= #
# ============================== #

catalogue_date = "05092024"
catalogue_label = "jiomart"
# history_date = "20220921_20221116"
# Variable.set("search_catalogue_date", catalogue_date)
catalogue_date_old = "05092024"  # Or Variable.get() the best Catalogue Date
testing = "false"

classPath = "ai.couture.obelisk.search.MainClass"
# code_artifact = "couture-search-engine-etl-2.0.0-bhavesh3.jar"#"couture-search-engine-etl-2.0.0-archita.jar"
code_artifact = "couture-search-engine-etl-2.0.0-tejkiran-jiomart.jar"
code_artifact_python = "__main__search_delta.py"
python_egg = "couture_search-jiomart-2.0.0-py3.11.egg"
python_commons_egg = "obelisk_retail_commons_piyush-1.0.1-py3.8.egg"
python_image = "couture/python-search-image:1.0.7"
kerberos_hosts = Variable.get("kerberos_hosts", deserialize_json=True)
searchEngineVolume = "searchEngineVolume:/home/pythondockervolume:Z"


# ======= Paths ======= #

str_date = ""
if len(str_date) == 0:
    str_date = datetime.now().strftime('%Y%m%d')
dirBasePath = "/data1/searchengine/"
dirPath = f"{dirBasePath}processed/{catalogue_label}/{catalogue_date}/"
dirPathProcessed = f"{dirPath}/etl/"
dirPathProcessedOld = f"{dirPath}/etl/"
# dirPathAnalysis = f"{dirBasePath}analysis/{catalogue_label}/{vertical}/deltacatalogue_{datetime.strptime(catalogue_date, '%d%m%Y').strftime('%Y%m%d')}/"

date = datetime(2018, 1, 2)

default_args = {
    "owner": "couture",
    "depends_on_past": False,
  	# "config_group": "config_group_search_engine_testing",
    # "config_group": "config_group_jiomart", 
    "config_group": "config_group_jiomart_large", 
    "start_date": date,
    "retries": 0,  # Temporarily kept because of "No Host supplied" error in WFO~, disabled since it does not help
    "retry_delay": timedelta(minutes=1),
    "code_artifact": code_artifact
}

Dag = DAG("search_engine_etl_jiomart_avinash", default_args=default_args, concurrency=4, schedule_interval=None, tags=["search-engine"])

environment = {
    "search_engine_etl_jiomart": "jiomart"
}


# ==================== #
# ======= LOAD ======= #
# ==================== #

# ======== LEVEL 0 ======== #

# bash_cmd_download = """
# echo "cd searchengine; bash download_ajio_catalogue.sh" | sshpass -p tailor@2021 ssh couture@10.144.96.176
# """

# GetCatalogueFromFTP = SSHOperator(
#     ssh_conn_id="AIRFLOW_CONN_SSH_SERVER",  # To connect to DBS
#     task_id=vertical_prefix+"GetCatalogueFromFTP",
#     command=bash_cmd_download,  # Runs these commands on the SSH"ed server (i.e. DBS)
#     # description="Fetches catalogue data shared by RRA from sftp",
#     dag=Dag
# )

PreProcessCatalogueData = CoutureSpark3Operator(
    task_id="PreProcessCatalogueData",
    method_id="PreProcessCatalogueData",
    class_path=classPath,
    code_artifact=code_artifact,
    method_args_dict={"min_catalogue_attributes_count": 100,
                      "min_product_count": 400000,
                      "min_brands_count": 3500,
                      "min_l1l2_categories_count": 70,
                      "min_l1l3_categories_count": 290,
                      "pg_table_catalogue": "project_catalogue",
                      "test": testing},
    input_base_dir_path=dirPath,
    output_base_dir_path=dirPathProcessed,
    input_filenames_dict={"catalogue_data": f"catalogue/FinalDF3"},  # .json"},
    output_filenames_dict={"processed_catalogue": "CatalogueAttributes"},
    dag=Dag,
    # config_group="config_group_jiomart",
    description="Converts json-ized catalogue data to parquet"
)
# ProcessCatalogueData.set_upstream([GetCatalogueFromFTP])

verticals = [
    # "Fashion",
    # "Electronics",
    # "Home & Lifestyle",
    # "Groceries",
    # "Industrial & Professional Supplies",
    # "Books, Music & Stationery",
    # "Furniture",
    "Beauty",
    # "Sports, Toys & Luggage",
    # "Wellness",
    # "Crafts of India",
    # "Precious Jewellery",
    # "Premium Fruits",
]

# we are trying to loop over the level2 for each vertical
# ======== LEVEL 2 ======== #

vertical_tasks = []

for vertical in verticals:

    dirPathProcessed_input = f"/data1/searchengine/processed/jiomart/05092024/etl/CatalogueAttributes/l1_name={vertical}/"
    dirPathAnalysis = f"{dirBasePath}analysis/{catalogue_label}/{vertical}/deltacatalogue_{datetime.strptime(catalogue_date, '%d%m%Y').strftime('%Y%m%d')}/"
    dirPathProcessed_output = f"{dirPath}/etl/{vertical}/"

    # changing the vertical path it replaces the space with underscore and & with space
    vertical_prefix = change_vertical_name(vertical)
    vertical_prefix += "."
    
    # Dummy task to trigger the DAG group
    triggerTask = DummyOperator(task_id=vertical_prefix+"TriggerTask",dag=Dag)

    with TaskGroup(f"{vertical_prefix}Group", dag=Dag) as vertical_group:
        GenerateCatalogueSummary = CoutureSpark3Operator(
            task_id=vertical_prefix+"GenerateCatalogueSummary",
            dag=Dag,
            # code_artifact="couture-search-engine-etl-2.0.0-bhavesh3.jar",
            class_path=classPath,
            method_id="GenerateCatalogueSummary",
            method_args_dict={"SUMMARYTYPE": "catalogue"},
            input_base_dir_path="",
            output_base_dir_path=dirPathAnalysis,
            input_filenames_dict={"option_catalogue_path": dirPathProcessed_input,
                                "option_catalogue_path_old": f"{dirPathProcessedOld}CatalogueAttributes"},
            output_filenames_dict={"summary_output": "Summary/CatalogueSummary"},
            # config_group="config_group_jiomart",
            description=""
        )
        # GenerateCatalogueSummary.set_upstream([PreProcessCatalogueData])

        DeriveCategoryAssociations = CoutureSpark3Operator(
            task_id=vertical_prefix+"DeriveCategoryAssociations",
            method_id="DeriveCategoryAssociations",
            class_path=classPath,
            # code_artifact="couture-search-engine-etl-2.0.0-bhavesh3.jar",
            method_args_dict={ "test": testing},
            input_base_dir_path="",
            output_base_dir_path=dirPathProcessed_output,
            input_filenames_dict={"catalogue_attributes": dirPathProcessed_input},
            output_filenames_dict={"cooccuring_l1l3": "cooccuringL1L3",
                                "category_mapping_l1l3_to_l1l2": "CategoryMappingL1L3ToL1L2",
                                "category_mapping_l1l2_to_l1l3": "CategoryMappingL1L2ToL1L3",
                                "category_counts": "CategoryCounts"},
            dag=Dag,
            # config_group="config_group_jiomart",
            description="Maps l1l2 to l1l3 and vice versa based on co-occurrence of categories for products"
        )

        # DeriveCategoryAssociations.set_upstream([PreProcessCatalogueData])

        ExtractBrands = CoutureSpark3Operator(
            task_id=vertical_prefix+"ExtractBrandsAndSpecialBrands",
            dag=Dag,
            # code_artifact="couture-search-engine-etl-2.0.0-bhavesh3.jar",
            class_path=classPath,
            method_id="ExtractBrandsAndSpecialBrands",
            method_args_dict={},
            # config_group="config_group_jiomart",
            input_base_dir_path="",
            output_base_dir_path=dirPathProcessed_output,
            input_filenames_dict={"product_attributes":dirPathProcessed_input},
            output_filenames_dict={"brands": "Brands",
                                "special_brands": "SpecialBrands"},
            description="Extracts brands from the catalogue data and recognizes "
                        "brands containing special characters in their name"
        )
        # ExtractBrands.set_upstream([PreProcessCatalogueData])

        TransposeCatalogueAttributes = CoutureSpark3Operator(
            task_id=vertical_prefix+"TransposeCatalogueAttributes",
            method_id="TransposeCatalogueAttributes",
            class_path=classPath,
            code_artifact=code_artifact,
            method_args_dict={"test": testing},
            input_base_dir_path="",
            output_base_dir_path=dirPathProcessed_output,
            input_filenames_dict={"catalogue_attributes":dirPathProcessed_input},
            output_filenames_dict={"transpose_catalogue_attributes": "TransposedCatalogueAttributesWithHierarchy",
                                "hard_filter_value_counts": "HardFilterValueCounts"},
            dag=Dag,
            description="Converts all the non-numeric attribute columns to transpose format"
        )
        # TransposeCatalogueAttributes.set_upstream([PreProcessCatalogueData])

        TransposeCatalogueNumericalAttributes = CoutureSpark3Operator(
            task_id=vertical_prefix+"TransposeCatalogueNumericalAttributes",
            dag=Dag,
            code_artifact=code_artifact,
            class_path=classPath,
            method_id="TransposeCatalogueNumericalAttributes",
            method_args_dict={},
            input_base_dir_path="",
            output_base_dir_path=dirPathProcessed_output,
            input_filenames_dict={"catalogue_attributes":dirPathProcessed_input},
            output_filenames_dict={"transpose_catalogue_attributes_numerical": "TransposedCatalogueAttributesNumerical"},
            description="Converts all the attribute columns having numeric related data to transpose format"
        )
        # TransposeCatalogueNumericalAttributes.set_upstream([PreProcessCatalogueData])


        GenerateCatalogueWordIndexes = CouturePythonDockerOperator(
            task_id=vertical_prefix+'GenerateCatalogueWordIndexes',
            image=python_image,
            api_version='auto',
            auto_remove=False,
            command="/bin/bash echo 'heloword'",
            extra_hosts=kerberos_hosts,
            user='couture',
            dag=Dag,
            mem_limit="18g",
            code_artifact=code_artifact_python,
            python_deps=[python_commons_egg, python_egg],
            method_id='generate_catalogue_word_index',
            method_args_dict={"pg_table_open_tokens": "project_opentokenindex"},
            volumes=[searchEngineVolume],
            input_base_dir_path=dirPathProcessed_output,
            output_base_dir_path=dirPathProcessed_output,
            input_filenames_dict={"catalogue": "TransposedCatalogueAttributesWithHierarchy",
                                "catalogue_numerical":"TransposedCatalogueAttributesNumerical"},
            output_filenames_dict={"filter_word_index": "CatalogueWordIndexes/filterWordIndex.pkl",
                                "open_token_index": "CatalogueWordIndexes/open_token_index.pkl"},
            description=''
        )

        GenerateCatalogueWordIndexes.set_upstream([TransposeCatalogueAttributes, TransposeCatalogueNumericalAttributes])
        
    PreProcessCatalogueData >> triggerTask >> vertical_group