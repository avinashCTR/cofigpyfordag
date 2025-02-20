import pprint
from datetime import datetime, timedelta

from CouturePythonDockerPlugin import CouturePythonDockerOperator
from CoutureSpark3Plugin import CoutureSpark3Operator as CoutureSparkOperator
from DagOperatorPlugin import DagOperator
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import Variable
from airflow.operators.python_operator import BranchPythonOperator
from airflow.utils.task_group import TaskGroup

import re

pp = pprint.PrettyPrinter(indent=4)

# ============================== #
# ======= Data Variables ======= #
# ============================== #

classPath = "ai.couture.obelisk.search.MainClass"
code_artifact = "couture-search-pipelines-2.0.0-tejkiran-jiomart.jar"
# code_artifact_meghana = "couture-search-pipelines-2.0.0-venkata.jar"
code_artifact_tejkiran = "couture-search-pipelines-2.0.0-tejkiran-jiomart.jar"
code_artifact_python = "__main__search_delta.py"
code_artifact_python_tejkiran = "__main__search_delta_tejkiran.py"
code_artifact_python_phase2 = '__main__search_phase2.py'
code_artifact_widget = "__main__widgets_meghana.py"
python_egg = "couture_search-2.0.7-py3.12.egg"  # "couture_search-2.0.0-py3.11.egg"
# python_egg_tejkiran = "couture_search-2.0.0-py3.12.egg"
# python_egg_meghana = "couture_search-2.0.0_test_meghana-py3.10.egg"
python_egg_phase2 = "couture_search-2.0.3-py3.12.egg"
python_commons_egg = "obelisk_retail_commons-1.0.1-py3.11.egg"
python_lumous_egg = "lumos-0.4-py3.12.egg"
python_core_egg = "couture_search_core-0.1-py3.8.egg"
python_image = "couture/python-search-image:1.1.2"
kerberos_hosts = Variable.get("kerberos_hosts", deserialize_json=True)
searchEngineVolume = "io_volume:/home/jovyan/io_volume:Z"

def task_group(dag,vertical,conf,paths_conf):

    # ======= Paths ======= #
    str_date = ""
    if len(str_date) == 0:
        str_date = datetime.now().strftime('%Y%m%d')
        
        
     # reading all the paths related variable configerations   
    dirBasePath = paths_conf["dirBasePath"]
    catalogue_label = paths_conf["catalogue_label"]
    catalogue_date = paths_conf["catalogue_date"]
    catalogue_date_old = paths_conf["catalogue_date_old"]
    version_new = paths_conf["version_new"]
    version_old = paths_conf["version_old"]
    testing = paths_conf["testing"]
    
    # constructing all the paths from the variables
    dirPath = f"{dirBasePath}processed/{catalogue_label}/{catalogue_date}"
    dirPathRawData = f"{dirBasePath}rawdata/"
    dirPathProcessed = f"{dirPath}"
    dirPathProcessedOld = f"{dirPath}/{version_old}/{vertical}/"
    dirPathAnalysis = f"{dirBasePath}analysis/{catalogue_label}/{vertical}/deltacatalogue_{datetime.strptime(catalogue_date, '%d%m%Y').strftime('%Y%m%d')}/"
    dirPathStaticData = f"{dirBasePath}staticdata/fashion/"
    dirPathJioMartStaticData = f"{dirBasePath}staticdata/jiomart/"
    dirCummulativePath = "/data1/searchengine/processed/jiomart/accumulateddata/"
    dirPathComparisons = f"{dirBasePath}corpuscomparisons/{catalogue_date_old}_{catalogue_date}"
    dirPathETL = f"{dirPath}/etl/{vertical}/"
    # dirPathProcessedHistory = f"{dirBasePath}processedHistory20240301/{catalogue_label}/"
    dirPathProcessedHistory = f"{dirBasePath}processedHistory/{catalogue_label}/sample/"
    catpath = f"{dirPath}/etl/CatalogueAttributes/l1_name={vertical}/"

    Dag = dag

    versions = {
        "search_engine_legos_jiomart_master": "V6_delta_changes",
    }

    environment = {
        "search_engine_legos_jiomart_master": "jiomart",
    }

    if Dag.dag_id in versions:
        version_new = versions[Dag.dag_id]
        dirPathProcessed += f"/{version_new}/{vertical}/"
        
    vertical_prefix = change_vertical_name(vertical)
    vertical_prefix += "."

    with TaskGroup(vertical_prefix+"wrappingGroup", dag=Dag) as wrappingGroup:
        with TaskGroup(vertical_prefix+"ExtractValidRightWords", dag=Dag) as ExtractValidRightWords:
            with TaskGroup(vertical_prefix+"ExtractRightWords", dag=Dag) as ExtractRightWords:
                ExtractCatalogueRightWords = CoutureSparkOperator(
                    task_id=vertical_prefix+"ExtractCatalogueRightWords",
                    dag=Dag,
                    code_artifact=code_artifact,
                    class_path=classPath,
                    method_id="ExtractAndFilterRightWords",
                    method_args_dict=conf["ExtractCatalogueRightWords"]["method_args_dict"],
                    input_base_dir_path="",
                    output_base_dir_path=dirPathProcessed,
                    input_filenames_dict={
                        "product_attributes_transpose": f"{dirPathETL}TransposedCatalogueAttributesWithHierarchy",
                        "word2vec": f"{dirPathStaticData}WordVectorsDFCommonCrawl",
                        "brands": f"{dirPathETL}Brands",
                        "static_tokens_words": f"{dirPathStaticData}StaticTokensWords",
                        "category_counts": f"{dirPathETL}CategoryCounts",
                        "incorrect_rightwords": f"{dirPathStaticData}ExternalIncorrectRightwords",
                        "non_category_words": f"{dirPathStaticData}NonCategoryWords"},
                    output_filenames_dict={"right_words_raw": "RightwordsRaw",
                                        "right_words": "Rightwords",
                                        "product_attributes_transpose_exploded": "ProductAttributesTransposeExploded",
                                        "products_with_words": "ProductsWithWords",
                                        "words_with_entities_per_category": "WordsWithEntitiesPerCategory"},
                    # config_group="config_group_search_engine_testing",
                    description="Extracts words from catalogue which may be correct"
                )

                GetBrandCollectionsAndStyleType = CoutureSparkOperator(
                    task_id=vertical_prefix+"GetBrandCollectionsAndStyleType",
                    dag=Dag,
                    code_artifact=code_artifact,
                    class_path=classPath,
                    method_id="GetBrandCollectionsAndStyleType",
                    method_args_dict={},
                    input_base_dir_path="",
                    output_base_dir_path=dirPathProcessed,
                    input_filenames_dict={"catalogue_attributes": catpath,
                                        "right_words": f"{dirPathProcessed}Rightwords",
                                        "right_words_raw": f"{dirPathProcessed}RightwordsRaw",
                                        "incorrect_rightwords": f"{dirPathStaticData}ExternalIncorrectRightwords"},
                    output_filenames_dict={"brand_collections_words": "BrandCollectionsWords",
                                        "brand_collections_phrases": "BrandCollectionsPhrases"},
                    description="Take the cat attr df, split name_text and count distinct brands per token, remove the ones which are not related to only one brand"
                )
                GetBrandCollectionsAndStyleType.set_upstream([ExtractCatalogueRightWords])

            CombineRightwords = CoutureSparkOperator(
                task_id=vertical_prefix+"CombineRightwords",
                dag=Dag,
                code_artifact=code_artifact,
                class_path=classPath,
                method_id="UnionTablesOnCommonCols",
                method_args_dict={
                    # "all_dfs": f"""{dirPathStaticData}StaticTokensWords,
                    # 			   {dirPathProcessed}Rightwords,
                    # 			   {dirPathProcessed}BrandCollectionsWords
                    #               """,
                "all_dfs": f"""{dirPathStaticData}StaticTokensWords,
                                {dirPathProcessed}Rightwords,
                                {dirPathProcessed}BrandCollectionsWords
                                """,
                    "primary_cols": "rightword",
                    "cols_handle_type": "reference",
                    "test": testing},
                input_base_dir_path="",
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"reference": f"{dirPathProcessed}Rightwords"},
                output_filenames_dict={"union_df": "RightwordsCombined"},
                description="Add brand collections, full brands, comparators and entity tokens"
            )
            CombineRightwords.set_upstream(#[AnalyseM2RMapping, 
                                            [GetBrandCollectionsAndStyleType])

            PluralizeWordsToCategoryWords = CouturePythonDockerOperator(
                task_id=vertical_prefix+"PluralizeWordsToCategoryWords",
                image=python_image,
                api_version="auto",
                auto_remove=False,
                extra_hosts=kerberos_hosts,
                user="couture",
                dag=Dag,
                mem_limit="18g",
                code_artifact=code_artifact_python,
                python_deps=[python_commons_egg, python_egg],
                method_id="pluralize_words_to_category_words",
                method_args_dict={"test": testing},
                input_base_dir_path=dirPathProcessed,
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"right_words": "RightwordsCombined"},#"RightwordsFinal"},
                output_filenames_dict={"plural_mappings": "PluralMappings",
                                    "plural_wrongwords_non_category": "PluralWrongwordsNonCategory"},
                description=""
            )
            # PluralizeWordsToCategoryWords.set_upstream([FilterMerchantCorrections])
            PluralizeWordsToCategoryWords.set_upstream([CombineRightwords])

        with TaskGroup(vertical_prefix+"ExtractValidPhrases", dag=Dag) as ExtractValidPhrases:
            with TaskGroup(vertical_prefix+"ExtractPhrases", dag=Dag) as ExtractPhrases:
                GeneratePhrasesMetaData = CoutureSparkOperator(
                    task_id=vertical_prefix+"GeneratePhrasesMetaData",
                    dag=Dag,
                    code_artifact=code_artifact,
                    class_path=classPath,
                    method_id="GeneratePhrasesMetaData",
                    method_args_dict={"test": testing},
                    input_base_dir_path=dirPathETL,
                    output_base_dir_path=dirPathProcessed,
                    input_filenames_dict={"product_attributes_transpose": "TransposedCatalogueAttributesWithHierarchy",
                                        # "product_attributes_transpose": "TransposedCatalogueAttributesWithHierarchy",
                                        "hard_filter_counts": "HardFilterValueCounts"},
                    output_filenames_dict={"phrases_metadata": "PhrasesMetaData"},
                    # config_group="config_group_search_engine",
                    description="Extracts properties for attribute values for phrases to consume"
                )

                ExtractPhrasesFromSpecificAttributes = CoutureSparkOperator(
                    task_id=vertical_prefix+"ExtractPhrasesFromSpecificAttributes",
                    dag=Dag,
                    code_artifact=code_artifact_tejkiran,
                    class_path=classPath,
                    method_id="ExtractPhrasesFromSpecificAttributes",
                    method_args_dict=conf["ExtractPhrasesFromSpecificAttributes"]["method_args_dict"],
                    input_base_dir_path="",
                    output_base_dir_path=dirPathProcessed,
                    input_filenames_dict={"phrases_meta_data": f"{dirPathProcessed}PhrasesMetaData",
                                        # "phrases_meta_data": f"{dirPathProcessed}PhrasesMetaData",
                                        "non_category_words": f"{dirPathStaticData}NonCategoryWords"},
                    output_filenames_dict={"phrases_from_specific_attributes": "PhrasesFromSpecificAttributes",
                                        "phrases_with_entities_per_category": "SpecificPhrasesWithEntitiesPerCategory"},
                    # config_group="config_group_search_engine",
                    description="Extracts multi-token phrases from specific attributes of catalogue "
                                "with some rules for each specific attribute"
                )
                ExtractPhrasesFromSpecificAttributes.set_upstream([GeneratePhrasesMetaData])
                
                GenerateDerivedPhrases = CoutureSparkOperator(
                    task_id=vertical_prefix+"GenerateDerivedPhrases",
                    dag=Dag,
                    code_artifact=code_artifact,
                    class_path=classPath,
                    method_id="GenerateDerivedPhrases",
                    method_args_dict=conf["GenerateDerivedPhrases"]["method_args_dict"],
                    input_base_dir_path="",
                    output_base_dir_path=f"{dirPathProcessed}",
                    input_filenames_dict={"right_words": f"{dirPathProcessed}" + "Rightwords",
                                        "category_plurals": f"{dirPathProcessed}" + "PluralMappings",
                                        "product_attributes_transpose_exploded": f"{dirPathProcessed}" + "ProductAttributesTransposeExploded",
                                        "category_counts": f"{dirPathETL}CategoryCounts",
                                        "static_tokens_words": f"{dirPathStaticData}StaticTokensWords",
                                        "entity_tokens": f"{dirPathStaticData}EntityWords"},
                    output_filenames_dict={"derived_phrases": "DerivedPhrases",
                                        "entities_per_category": "DerivedPhrasesWithEntitiesPerCategory"},
                    # config_group="config_group_search_engine",
                    description=""
                )
                GenerateDerivedPhrases.set_upstream([ExtractCatalogueRightWords, ExtractValidRightWords])

            CombinePhrases = CoutureSparkOperator(
                task_id=vertical_prefix+"CombinePhrases",
                dag=Dag,
                code_artifact=code_artifact,
                class_path=classPath,
                method_id="UnionTablesOnCommonCols",
                method_args_dict={
                    "all_dfs": f"""{dirPathProcessed}DerivedPhrases, 
                                {dirPathProcessed}PhrasesFromSpecificAttributes, 
                                {dirPathStaticData}StaticTokensPhrases""",
                                # {dirPathProcessed}NewPhrasesWithProperties,
                                # {dirPathProcessed}DerivedPhrases,
                                # {dirPathProcessed}PhrasesFromSpecificAttributes,
                                # {dirPathProcessed}CataloguePhrasesFiltered,
                                # {dirPathProcessed}BrandCollectionsPhrases,
                                # {dirPathStaticData}StaticTokensPhrases
                                # """,
                    #           {dirPathProcessed}PhrasesFromSpecificAttributes,
                    "primary_cols": "phrase",
                    "cols_handle_type": "reference",
                    "test": testing},
                input_base_dir_path="",
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"reference": f"{dirPathProcessed}PhrasesFromSpecificAttributes"},
                output_filenames_dict={"union_df": "CombinedPhrases"},
                # config_group="config_group_search_engine",
                description="Combines Phrases from multiple sources"
            )
            CombinePhrases.set_upstream([ExtractPhrases])

            MergeMultiplePhraseKeys = CoutureSparkOperator(
                task_id=vertical_prefix+"MergeMultiplePhraseKeys",
                dag=Dag,
                code_artifact=code_artifact,
                class_path=classPath,
                method_id="MergeMultiplePhraseKeys",
                method_args_dict={"test": testing},
                input_base_dir_path=dirPathProcessed,
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"combined_phrases": "CombinedPhrases"},
                output_filenames_dict={"merged_phrases": "MergedPhrases"},
                description="Merges multiple rows of duplicated phrase_keys into a single row"
            )
            MergeMultiplePhraseKeys.set_upstream([CombinePhrases])

        CombineEntityCorpuses = CoutureSparkOperator(
            task_id=vertical_prefix+"CombineEntityCorpuses",
            method_id="UnionTablesOnCommonCols",
            class_path=classPath,
            code_artifact=code_artifact,
            method_args_dict={
                "all_dfs": """WordsWithEntitiesPerCategory,
                            DerivedPhrasesWithEntitiesPerCategory,
                            SpecificPhrasesWithEntitiesPerCategory
                            """,
                            # EntityCorpusForNewPhrases,
            #### MODIFIED FOR PHASE 2 GUARDRAILS ON 25 JULY BY ARCHITA ####
                            # CataloguePhrasesWithEntitiesPerCategory
                            # """,
                "primary_cols": "token,category",
                "cols_handle_type": "intersect",
                "test": testing},
            input_base_dir_path=dirPathProcessed,
            output_base_dir_path=dirPathProcessed,
            input_filenames_dict={},
            output_filenames_dict={"union_df": "EntityCorpusCombined"},
            dag=Dag,
            description="Generates the Entity Corpus by combining the entity corpuses of"
                        "both words and phrases"
        )
        CombineEntityCorpuses.set_upstream([MergeMultiplePhraseKeys])

        ModifyEntityCorpus = CoutureSparkOperator(
            task_id=vertical_prefix+"ModifyEntityCorpus",
            method_id="ModifyEntityCorpus",
            class_path=classPath,
            code_artifact=code_artifact,
            method_args_dict={"test": testing},
            input_base_dir_path="",
            output_base_dir_path=dirPathProcessed,
            input_filenames_dict={"entity_corpus_combined": f"{dirPathProcessed}EntityCorpusCombined",
                                "right_words_final": f"{dirPathProcessed}RightwordsCombined",
                                "phrases_final": f"{dirPathProcessed}MergedPhrases",
                                "category_plurals": f"{dirPathProcessed}PluralMappings",
        #                           "external_synonyms": f"{dirPathStaticData}Synonyms/External Synonyms.csv"},
                                "external_synonyms": f"{dirPathJioMartStaticData}Synonyms/External Synonyms.csv"},
            output_filenames_dict={"entity_corpus": "EntityCorpus",
                                "token_entities": "TokenEntities"},
            dag=Dag,
            description="Modifies and also adds few entries to entitycorpus based on category plurals"
        )
        ModifyEntityCorpus.set_upstream([CombineEntityCorpuses])

        with TaskGroup(vertical_prefix+"GenerateNumericCorpus", dag=Dag) as GenerateNumericCorpus:
            PopulateNumericalEntitiesRangeWise = CoutureSparkOperator(
                task_id=vertical_prefix+"PopulateNumericalEntitiesRangeWise",
                dag=Dag,
                code_artifact=code_artifact,
                class_path=classPath,
                method_id="PopulateEntitiesRangeWise",
                method_args_dict={},
                input_base_dir_path=dirPathETL,
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"product_attributes_transpose": "TransposedCatalogueAttributesNumerical"},
                output_filenames_dict={"numerical_entities_brick_wise": "NumericalEntitiesHierarchyWise",
                                    "transpose_catalogue_with_numbers": "TransposedCatalogueAttributesWithNumber"},
                description="Extracts and forms numeric ranges for entities per category"
            )

        with TaskGroup(vertical_prefix+"GenerateSynonyms", dag=Dag) as GenerateSynonyms:
            GetBrandFamilySynonyms = CoutureSparkOperator(
                task_id=vertical_prefix+"GetBrandFamilySynonyms",
                method_id="GetBrandFamilySynonyms",
                class_path=classPath,
                code_artifact=code_artifact,
                method_args_dict=conf["GetBrandFamilySynonyms"]["method_args_dict"],
                input_base_dir_path=dirPathETL,
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"brands": "Brands"},
                output_filenames_dict={"brand_family_synonyms": "BrandFamilySynonyms"},
                dag=Dag,
                description=""
            )

            GenerateSimilarWords = CoutureSparkOperator(
                task_id=vertical_prefix+"GenerateSimilarWords",
                method_id="IdentifyNeighbouringValues",
                class_path=classPath,
                code_artifact=code_artifact,
                method_args_dict=conf["GenerateSimilarWords"]["method_args_dict"],
                input_base_dir_path=dirPathProcessed,
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"id_features": "Rightwords"},
                output_filenames_dict={"similar_item_values": "SimilarWordsWithinCorpusNew"},
                dag=Dag,
                description=""
            )
            GenerateSimilarWords.set_upstream([ExtractRightWords])

            MapSimilarWordsToCategoryWords = CoutureSparkOperator(
                task_id=vertical_prefix+"MapSimilarWordsToCategoryWords",
                dag=Dag,
                code_artifact=code_artifact,
                class_path=classPath,
                method_id="MapSimilarWordsToCategoryWords",
                method_args_dict={},
                input_base_dir_path=dirPathProcessed,
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"similar_words": "SimilarWordsWithinCorpusNew",
                                    "right_words": "Rightwords"},
                output_filenames_dict={"category_synonyms": "CategorySynonyms"},
                description=""
            )
            MapSimilarWordsToCategoryWords.set_upstream([GenerateSimilarWords])

            FilterCategorySynonyms = CoutureSparkOperator(
                task_id=vertical_prefix+"FilterCategorySynonyms",
                dag=Dag,
                code_artifact=code_artifact,
                class_path=classPath,
                method_id="FilterCategorySynonyms",
                method_args_dict={},
                input_base_dir_path=dirPathProcessed,
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"product_attributes_transpose_exploded": "ProductAttributesTransposeExploded",
                                    "plural_mappings": "PluralMappings",
                                    "category_synonyms": "CategorySynonyms",
                                    "right_words": "Rightwords"},
                output_filenames_dict={"filtered_category_synonyms": "CategorySynonymsFiltered"},
                description=""
            )
            FilterCategorySynonyms.set_upstream([MapSimilarWordsToCategoryWords, ExtractCatalogueRightWords, PluralizeWordsToCategoryWords])

            GetColourSynonyms = CoutureSparkOperator(
                task_id=vertical_prefix+'GetColourSynonyms',
                dag=Dag,
                code_artifact=code_artifact,
                class_path=classPath,
                method_id='GetColourSynonyms',
                method_args_dict= {"lemmatizer_model_path": dirPathRawData + "models/lemma"},
                input_base_dir_path= "",
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"colours_with_synonyms": f"{dirPathStaticData}SynonymsRefining/ColoursWithSynonyms",
        #                               "rightwords": f"{dirPathProcessed}RightwordsFinal"},
                                    "rightwords": f"{dirPathProcessed}RightwordsCombined"},
                output_filenames_dict={"colour_synonyms": "ColourSynonyms"},
                # config_group="config_group_search_engine",
                description=''
            )
            # GetColourSynonyms.set_upstream([FilterMerchantCorrections])

        with TaskGroup(vertical_prefix+"GenerateSplitPhrases", dag=Dag) as GenerateSplitPhrases:
            SplitWords = CouturePythonDockerOperator(
                task_id=vertical_prefix+"SplitRightWords",
                image=python_image,
                api_version="auto",
                auto_remove=False,
                user="couture",
                extra_hosts=kerberos_hosts,
                dag=Dag,
                mem_limit="18g",
                code_artifact=code_artifact_python,
                python_deps=[python_commons_egg, python_egg],
                method_id="split_right_words",
                method_args_dict=conf["SplitWords"]["method_args_dict"],
                input_base_dir_path="",
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={#"rightwords": f"{dirPathProcessed}RightwordsFinal",
                                    "rightwords": f"{dirPathProcessed}RightwordsCombined",
                                    "stop_words": f"{dirPathStaticData}StopWords"},
                output_filenames_dict={"splitted_right_words": "SplittedRightWords",
                                    "int_op": "IntermediateOPbrands"},
                description=""
            )

            FilterSplitWordPhrases = CoutureSparkOperator(
                task_id=vertical_prefix+"FilterSplitWordPhrases",
                dag=Dag,
                code_artifact=code_artifact,
                class_path=classPath,
                method_id="FilterSplitWordPhrases",
                method_args_dict={"test": testing},
                input_base_dir_path=dirPathProcessed,
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"phrases_with_splitted_words": "SplittedRightWords",
                                    "token_entities": "TokenEntities",
                                    "right_words": "RightwordsCombined"},
                output_filenames_dict={"phrases_op": "SplittedRightWordsFiltered"},
                description=""
            )
            FilterSplitWordPhrases.set_upstream([SplitWords, ModifyEntityCorpus])

            FilterRightQueriesFromHistory = CoutureSparkOperator(
                task_id=vertical_prefix+"FilterRightQueriesFromHistory",
                method_id="FilterRightQueriesFromHistory",
                class_path=classPath,
                code_artifact=code_artifact,
                method_args_dict=conf["FilterRightQueriesFromHistory"]["method_args_dict"],
                input_base_dir_path="",
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"history_queries": f"{dirPathProcessedHistory}QueryClicksWithFrequencies",
                                    "right_words": f"{dirPathProcessed}RightwordsCombined"},
        #                               "right_words": f"{dirPathProcessed}RightwordsFinal"},
                output_filenames_dict={"history_right_queries": "HistoryRightQueries"},
                dag=Dag,
                description="Takes cleaned/normalised history queries (preferably accumulated) and identifies the most popular split words"
            )

            GenerateUserSplitWords = CouturePythonDockerOperator(
                task_id=vertical_prefix+"GenerateUserSplitWords",
                image=python_image,
                api_version="auto",
                auto_remove=False,
                extra_hosts=kerberos_hosts,
                user="couture",
                dag=Dag,
                code_artifact=code_artifact_python,
                python_deps=[python_commons_egg, python_egg],
                method_id="generate_user_splitted_words",
                method_args_dict={"test": testing},
                input_base_dir_path=dirPathProcessed,
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"right_queries": "HistoryRightQueries",
                                    "filtered_phrases": "BrickWisePhrases",  # TODO: Change it to "FilteredPhrases",
                                    # "right_words": "RightwordsFinal"},
                                    "right_words": "RightwordsCombined"},
                output_filenames_dict={"user_split_words": "UserSplitWords"},
                description=""
            )
            GenerateUserSplitWords.set_upstream([FilterRightQueriesFromHistory])

        with TaskGroup(vertical_prefix+"RefineTokenCategories", dag=Dag) as RefineTokenCategories:
            FilterCategoriesForPhrases = CoutureSparkOperator(
                task_id=vertical_prefix+"FilterCategoriesForPhrases",
                dag=Dag,
                code_artifact=code_artifact,
                class_path=classPath,
                method_id="FilterCategoriesForWords",
                method_args_dict=conf["FilterCategoriesForWords"]["method_args_dict"],
                input_base_dir_path="",
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"right_words": f"{dirPathProcessed}MergedPhrases",
                                    "category_mapping_l1l3_to_l1l2": f"{dirPathETL}CategoryMappingL1L3ToL1L2",
                                    "category_mapping_l1l2_to_l1l3": f"{dirPathETL}CategoryMappingL1L2ToL1L3",
                                    "plural_mapping": f"{dirPathProcessed}PluralMappings",
                                    "split_words": f"{dirPathProcessed}SplittedRightWords",
                                    "user_splitted_words": f"{dirPathProcessed}UserSplitWords"},
                output_filenames_dict={"rightwords_category_filtered": "PhrasesCategoryFiltered"},
                description=""
            )
            FilterCategoriesForPhrases.set_upstream([MergeMultiplePhraseKeys])

            FilterCategoriesForWords = CoutureSparkOperator(
                task_id=vertical_prefix+"FilterCategoriesForWords",
                dag=Dag,
                code_artifact=code_artifact,
                class_path=classPath,
                method_id="FilterCategoriesForWords",
                method_args_dict=conf["FilterCategoriesForWords"]["method_args_dict"],
                input_base_dir_path="",
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"right_words": f"{dirPathProcessed}RightwordsCombined",
                                    #"right_words": f"{dirPathProcessed}RightwordsFinal",
                                    "category_mapping_l1l3_to_l1l2": f"{dirPathETL}CategoryMappingL1L3ToL1L2",
                                    "category_mapping_l1l2_to_l1l3": f"{dirPathETL}CategoryMappingL1L2ToL1L3",
                                    "plural_mapping": f"{dirPathProcessed}PluralMappings",
                                    "split_words": f"{dirPathProcessed}SplittedRightWords",
                                    "user_splitted_words": f"{dirPathProcessed}UserSplitWords"},
                output_filenames_dict={"rightwords_category_filtered": "RightWordsCategoryFiltered"},
                description=""
            )

        RefineTokenCategories.set_upstream([GenerateSplitPhrases, ExtractValidRightWords])

        CombineWordProperties = CoutureSparkOperator(
            task_id=vertical_prefix+'CombineWordProperties',
            dag=Dag,
            code_artifact=code_artifact,
            class_path=classPath,
            method_id='CombineTokenProperties',
            method_args_dict={"token_col": "rightword",
                            "test": testing},
            input_base_dir_path= "",
            output_base_dir_path=dirPathProcessed,
            input_filenames_dict={"l1_synonyms": f"{dirPathStaticData}Synonyms/L1SynonymsAndPronouns",
                                "category_synonyms": f"{dirPathProcessed}CategorySynonymsFiltered",
                                #"right_tokens": f"{dirPathProcessed}RightWordsWithHistoryCategories",
                                "right_tokens": f"{dirPathProcessed}RightWordsCategoryFiltered",
                                "token_entities": f"{dirPathProcessed}TokenEntities",
                                # "synonyms": f"{dirPathProcessed}SimilarWordsWithinCorpusNew",
                                "brand_family_synonyms": f"{dirPathProcessed}BrandFamilySynonyms",
                                "color_synonyms": f"{dirPathProcessed}ColourSynonyms",
                                "attribute_synonyms": f"{dirPathProcessed}ExistingAttributeSynonyms",
                                "non_category_plurals": f"{dirPathProcessed}PluralWrongwordsNonCategory",
                                "eng_uk_to_us": f"{dirPathJioMartStaticData}Synonyms/uk-to-us-eng.tsv",
                                "external_synonyms": f"{dirPathJioMartStaticData}Synonyms/External Synonyms.csv",
                                "same_attribute_values": f"{dirPathStaticData}Synonyms/SameAttributeValues.csv"
                                },
            # "brand_substitutions": f"{dirPathProcessed}InternalBrandSubstitutes"},
            output_filenames_dict={"token_translated": "RightwordsTranslated",
                                "additional_tokens": "AdditionalRightwords"},
            # config_group="config_group_search_engine",
            description=''
        )
        # CombineWordProperties.set_upstream([GenerateSynonyms, AddHistoryCategoriesToWords]) #GenerateSubstitutes
        CombineWordProperties.set_upstream([GenerateSynonyms]) #GenerateSubstitutes

        CombinePhraseProperties = CoutureSparkOperator(
            task_id=vertical_prefix+'CombinePhraseProperties',
            dag=Dag,
            code_artifact=code_artifact,
            class_path=classPath,
            method_id='CombineTokenProperties',
            method_args_dict={"token_col": "phrase",
                            "test": testing},
            input_base_dir_path= "",
            output_base_dir_path=dirPathProcessed,
            input_filenames_dict={"l1_synonyms": f"{dirPathStaticData}Synonyms/L1SynonymsAndPronouns",
                                "category_synonyms": f"{dirPathProcessed}CategorySynonymsFiltered",
                                #"right_tokens": f"{dirPathProcessed}PhrasesWithHistoryCategories",
                                "right_tokens": f"{dirPathProcessed}PhrasesCategoryFiltered",
                                "token_entities": f"{dirPathProcessed}TokenEntities",
                                # "synonyms": f"{dirPathProcessed}SimilarWordsWithinCorpusNew",
                                "brand_family_synonyms": f"{dirPathProcessed}BrandFamilySynonyms",
                                "color_synonyms": f"{dirPathProcessed}ColourSynonyms",
                                "attribute_synonyms": f"{dirPathProcessed}ExistingAttributeSynonyms",
                                "non_category_plurals": f"{dirPathProcessed}PluralWrongwordsNonCategory",
                                "eng_uk_to_us": f"{dirPathJioMartStaticData}Synonyms/uk-to-us-eng.tsv",
                                "external_synonyms": f"{dirPathJioMartStaticData}Synonyms/External Synonyms.csv",
                                "same_attribute_values": f"{dirPathStaticData}Synonyms/SameAttributeValues.csv"
                                },
            # "brand_substitutions": f"{dirPathProcessed}InternalBrandSubstitutes"},
            output_filenames_dict={"token_translated": "PhrasesTranslated",
                                "additional_tokens": "AdditionalPhrases"},
            description=''
        )
        # CombinePhraseProperties.set_upstream([GenerateSynonyms, AddHistoryCategoriesToPhrases]) #GenerateSubstitutes
        CombinePhraseProperties.set_upstream([GenerateSynonyms]) #GenerateSubstitutes


        with TaskGroup(vertical_prefix+"GenerateWordVariants", dag=Dag) as GenerateWordVariants:
            FilterExternalVariants = CoutureSparkOperator(
                task_id=vertical_prefix+"FilterExternalVariants",
                dag=Dag,
                code_artifact=code_artifact,
                class_path=classPath,
                method_id="FilterExternalVariants",
                method_args_dict=conf["FilterExternalVariants"]["method_args_dict"],
                input_base_dir_path="",
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"external_vocabulary_words": f"{dirPathJioMartStaticData}ExternalVocabularyWords",
                                    "manually_tagged_words": f"{dirPathJioMartStaticData}ExternalMappings",
                                    "hinglish_variants": f"{dirPathJioMartStaticData}HinglishVariants",
                                    "processed_attribute_synonyms": f"{dirPathJioMartStaticData}ProcessedAttributeSynonyms",
                                    #"right_words": f"{dirPathProcessed}RightwordsFinal",
                                    "right_words": f"{dirPathProcessed}RightwordsCombined",
                                    "filtered_phrases": f"{dirPathProcessed}MergedPhrases"},
                output_filenames_dict={"wrong_to_right_words_external_variants": "WordsExternalVariants",
                                    "external_phrase_variants": "PhrasesExternalVariants",
                                    "existing_attribute_synonyms": "ExistingAttributeSynonyms"},
                description=""
            )
            FilterExternalVariants.set_upstream([CombineEntityCorpuses])

            GeneratePhoneticVariants = CouturePythonDockerOperator(
                task_id=vertical_prefix+"GeneratePhoneticVariants",
                image=python_image,
                api_version="auto",
                auto_remove=False,
                user="couture",
                extra_hosts=kerberos_hosts,
                dag=Dag,
                mem_limit="18g",
                code_artifact=code_artifact_python,
                python_deps=[python_commons_egg, python_egg],
                method_id="generate_phonetic_variants",
                method_args_dict=conf["GeneratePhoneticVariants"]["method_args_dict"],
                input_base_dir_path=dirPathProcessed,
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"right_words": "RightwordsCombined"},#"RightwordsFinal"},
                output_filenames_dict={"phonetic_variants": "PhoneticVariants"},
                description=""
            )

            GenerateSpellVariants = CoutureSparkOperator(
                task_id=vertical_prefix+"GenerateSpellVariantsForWords",
                dag=Dag,
                code_artifact=code_artifact,
                class_path=classPath,
                method_id="GenerateSpellVariants",
                # method_args_dict={"hierarchy": "colorGroup_string"},
                method_args_dict={"hierarchy": "objectID"},
                input_base_dir_path=dirPathProcessed,
                output_base_dir_path=dirPathProcessed,
                # input_filenames_dict={"right_words": "RightwordsFinal"},
                input_filenames_dict={"right_words": "RightwordsCombined"},#"Rightwords"},
                output_filenames_dict={"wrong_to_right_words_spell_variants": "SpellVariants"},
                description=""
            )

            GenerateLemmatizedVariants = CouturePythonDockerOperator(
                task_id=vertical_prefix+"GenerateLemmatizedVariants",
                image=python_image,
                api_version="auto",
                auto_remove=False,
                extra_hosts=kerberos_hosts,
                user="couture",
                dag=Dag,
                mem_limit="18g",
                code_artifact=code_artifact_python,
                python_deps=[python_commons_egg, python_egg],
                method_id="generate_lemmatized_variants",
                method_args_dict={},
                input_base_dir_path=dirPathProcessed,
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"right_words": "RightwordsCombined"},#"RightwordsFinal"},
                output_filenames_dict={"lemmatized_variants": "LemmatizedVariants"},
                description=""
            )

            CombineWordVariants = CoutureSparkOperator(
                task_id=vertical_prefix+"CombineWordVariants",
                dag=Dag,
                code_artifact=code_artifact,
                class_path=classPath,
                method_id="UnionTablesOnCommonCols",
                method_args_dict={
                    "all_dfs": """WordsExternalVariants,
                                SpellVariants,
                                LemmatizedVariants""",
                                # PhoneticVariants""",
                    "primary_cols": "wrongword,rightword",
                    "cols_handle_type": "intersect",
                    "test": testing},
                input_base_dir_path=dirPathProcessed,
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={},
                output_filenames_dict={"union_df": "W2RCombinedVariants"},
                # config_group="config_group_search_engine",
                description="Extracts properties for attribute values for phrases to consume"
            )
            # CombineWordVariants.set_upstream([FilterExternalVariants, GeneratePhoneticVariants, GenerateSpellVariants, GenerateLemmatizedVariants])
            CombineWordVariants.set_upstream([FilterExternalVariants, GenerateSpellVariants, GenerateLemmatizedVariants])
            GenerateWordVariants.set_upstream([CombineRightwords])

        with TaskGroup(vertical_prefix+"RemoveWrongW2RMappings", dag=Dag) as RemoveWrongW2RMappings:

            RemoveExternalPhraseVariantsFromW2R = CoutureSparkOperator(
                task_id=vertical_prefix+"RemoveExternalPhraseVariantsFromW2R",
                dag=Dag,
                code_artifact=code_artifact,
                class_path=classPath,
                method_id="RemoveExternalPhraseVariantsFromW2R",
                method_args_dict={},
                input_base_dir_path=dirPathProcessed,
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={
        #         "wrong_to_right_words": "W2RAllVariants",
                "wrong_to_right_words": "W2RCombinedVariants",
                                    "phrases_external_variants": "PhrasesExternalVariants",
                                    "category_plurals": "PluralMappings",
                                    "non_category_plurals": "PluralWrongwordsNonCategory"},
                output_filenames_dict={"wrong_to_right_words": "W2RAllVariantsCleaned"},
                description=""
            )

        RemoveWrongW2RMappings.set_upstream([GenerateWordVariants])

        with TaskGroup(vertical_prefix+"CalculateCombinedScoreW2R", dag=Dag) as CalculateCombinedScoreW2R:
            GenerateIPATransliterations = CouturePythonDockerOperator(
                task_id=vertical_prefix+"GenerateIPATransliterations",
                image=python_image,
                api_version="auto",
                auto_remove=False,
                user="couture",
                mem_limit="18g",
                extra_hosts=kerberos_hosts,
                dag=Dag,
                code_artifact="__main__search_delta.py",
                python_deps=[python_commons_egg, python_egg],
                method_id="generate_ipa_transliterations",
                method_args_dict=conf["GenerateIPATransliterations"]["method_args_dict"],
                input_base_dir_path=dirPathProcessed,
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"wrong_to_right_words": "W2RAllVariantsCleaned",
                                    "ipa_meta_data": f"{dirCummulativePath}IPATransliterationsAutomaticAccumulator20250117"},
                output_filenames_dict={"wrong_to_right_words_scored": "W2RWithIPATransliterations",
                                    "ipa_meta_data_accumulated": f"{dirCummulativePath}IPATransliterationsAutomaticAccumulator{datetime.now().strftime('%Y%m%d')}Out"},
                description=""
            )
            CalculateDamerauLevenshteinSimilarity = CoutureSparkOperator(
                task_id=vertical_prefix+"CalculateDamerauLevenshteinSimilarity",
                dag=Dag,
                code_artifact=code_artifact,
                class_path=classPath,
                method_id="CalculateDamerauLevenshteinSimilarity",
                method_args_dict=conf["CalculateDamerauLevenshteinSimilarity"]["method_args_dict"],
                input_base_dir_path="",
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={
        #             "right_words": f"{dirPathProcessed}RightwordsFinal",
                    "right_words": f"{dirPathProcessed}RightwordsCombined",
                    "w2r_combined_variants": f"{dirPathProcessed}W2RWithIPATransliterations",
                    "stop_words": f"{dirPathStaticData}StopWords",
                    "brand_collection": f"{dirPathProcessed}BrandCollectionsWords",
                    "brands": f"{dirPathProcessed}Brands"},
                output_filenames_dict={"wrong_to_right_words": "W2RDamerauLevenshteinScored"},
                # config_group="config_group_search_engine",
                description=""
            )
            CalculateDamerauLevenshteinSimilarity.set_upstream([GenerateIPATransliterations])

            GetW2RScoreFromHistory = CoutureSparkOperator(
                task_id=vertical_prefix+"GetW2RScoreFromHistory",
                method_id="AnalyseW2RMapping",
                class_path=classPath,
                code_artifact=code_artifact,
                method_args_dict=conf["GetW2RScoreFromHistory"]["method_args_dict"],
                input_base_dir_path="",
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={
                    "transpose_product_attributes": f"{dirPathProcessed}ProductAttributesTransposeExploded",
                    "history_data": f"{dirPathProcessedHistory}QueryClicksWithFrequencies",
                    "wrong_to_right_mappings": f"{dirPathProcessed}W2RAllVariantsCleaned"},
                output_filenames_dict={"w2r_mapping_precision": "W2RHistoryScored"},
                dag=Dag,
                description=""
            )

            CalculateFinalScore = CoutureSparkOperator(
                task_id=vertical_prefix+"CalculateFinalScore",
                method_id="CalculateFinalScore",
                class_path=classPath,
                code_artifact=code_artifact,
                method_args_dict=conf["CalculateFinalScore"]["method_args_dict"],
                input_base_dir_path=dirPathProcessed,
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"w2r_levenshtein_sim": "W2RDamerauLevenshteinScored",
                                    "w2r_phonetic_sim": "W2RDamerauLevenshteinScored",
                                    "w2r_history_precision": "W2RHistoryScored",
        #                               "right_words": "RightwordsFinal"},
                                    "right_words": "RightwordsCombined"},
                output_filenames_dict={"wrong_to_right_words_final_scored": "W2RFinalScored",
                                    "wrong_to_right_words_1_to_1": "W2RFinal1To1"},
                dag=Dag,
                description=""
            )
            CalculateFinalScore.set_upstream([GetW2RScoreFromHistory,
                                            CalculateDamerauLevenshteinSimilarity])
        CalculateCombinedScoreW2R.set_upstream([RemoveWrongW2RMappings])

        with TaskGroup(vertical_prefix+"GeneratePhraseVariants", dag=Dag) as GeneratePhraseVariants:
            CleanAndNormalisePhrases = CoutureSparkOperator(
                task_id=vertical_prefix+"CleanAndNormalisePhrases",
                method_id="CleanAndNormaliseHistoryData",
                class_path=classPath,
                code_artifact=code_artifact,
                method_args_dict={"query_col": "phrase",
                                "test": testing},
                input_base_dir_path=dirPathProcessed,
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"all_raw_queries": "MergedPhrases"},
                output_filenames_dict={"history_queries_normalised": "NormalisedPhrases"},
                dag=Dag,
                description=""
            )
            CleanAndNormalisePhrases.set_upstream([MergeMultiplePhraseKeys])

            SpellCheckPhrases = CoutureSparkOperator(
                task_id=vertical_prefix+"SpellCheckPhrases",
                method_id="SpellCheckHistoryData",
                class_path=classPath,
                code_artifact=code_artifact,
                method_args_dict={"query_col": "phrase"},
                input_base_dir_path=dirPathProcessed,
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"history_queries_normalised": "NormalisedPhrases",
                                    "wrong_to_right_words": "W2RFinal1To1",
                                    # "right_words": "RightwordsFinal",
                                    "right_words": "RightwordsCombined",
                                    "user_splitted_words": "UserSplitWords",
                                    "splitted_right_words": "SplittedRightWords"},
                output_filenames_dict={"spell_corrected_queries": "SpellCorrectedPhrases",
                                    "interm_op": "allQueriesMapped"},
                dag=Dag,
                description=""
            )
            SpellCheckPhrases.set_upstream([CleanAndNormalisePhrases, CalculateCombinedScoreW2R, GenerateSplitPhrases])

            GeneratePhraseKeyVariants = CoutureSparkOperator(
                task_id=vertical_prefix+"GeneratePhraseKeyVariants",
                dag=Dag,
                code_artifact=code_artifact,
                class_path=classPath,
                method_id="GeneratePhraseVariants",
                method_args_dict={"test": testing},
                input_base_dir_path=dirPathProcessed,
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"phrases": "MergedPhrases",
                                    "spell_corrected_phrases": "SpellCorrectedPhrases"},
                output_filenames_dict={"phrases_with_variants": "PhrasesVariants"},
                description=""
            )
            GeneratePhraseKeyVariants.set_upstream([SpellCheckPhrases])
            
            GenerateInflectedVariantsForPhrases = CouturePythonDockerOperator(
                task_id=vertical_prefix+"GenerateInflectedVariantsForPhrases",
                image=python_image,
                api_version="auto",
                auto_remove=False,
                extra_hosts=kerberos_hosts,
                user="couture",
                dag=Dag,
                code_artifact=code_artifact_python,
                python_deps=[python_commons_egg, python_egg],
                method_id="generate_inflected_variants_for_phrases",
                method_args_dict={"test": testing},
                input_base_dir_path="",
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"phrases": f"{dirPathProcessed}CombinedPhrases",
                                    "static_words": f"{dirPathStaticData}StaticTokensWords"},
                output_filenames_dict={"inflected_phrases_variants": "InflectedPhrasesVariants"},
                description=""
            )
            GenerateInflectedVariantsForPhrases.set_upstream([CombinePhrases])

            PermutePluralWordsInPhrases = CouturePythonDockerOperator(
                task_id=vertical_prefix+"PermutePluralWordsInPhrases",
                image=python_image,
                api_version="auto",
                auto_remove=False,
                extra_hosts=kerberos_hosts,
                user="couture",
                dag=Dag,
                code_artifact=code_artifact_python,
                python_deps=[python_commons_egg, python_egg],
                method_id="permute_plural_words_in_phrases",
                method_args_dict={"test": testing},
                input_base_dir_path="",
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"phrases_with_synonyms": f"{dirPathProcessed}MergedPhrases",
                                    "stopwords": f"{dirPathStaticData}StopWords"},
                output_filenames_dict={"phrases_pluralized": "PhrasesPluralized"},
                description=""
            )
            PermutePluralWordsInPhrases.set_upstream([MergeMultiplePhraseKeys])

            GenerateBrandChunks = CoutureSparkOperator(
                task_id=vertical_prefix+"GenerateBrandChunks",
                method_id="GenerateBrandChunks",
                class_path=classPath,
                code_artifact=code_artifact,
                method_args_dict={"test": testing},
                input_base_dir_path="",
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"brands": f"{dirPathETL}Brands",
                                    "phrases": f"{dirPathProcessed}MergedPhrases",
                                    "brand_acronyms": f"{dirPathJioMartStaticData}BrandAcronyms",
                                    "right_words": f"{dirPathProcessed}Rightwords",
                                    "stop_words": f"{dirPathStaticData}StopWords",
                                    "phrases_meta_data": f"{dirPathProcessed}PhrasesMetaData"},
                output_filenames_dict={"brand_chunks": "BrandChunks",
                                    "brand_acronyms": "BrandAcronyms"},
                dag=Dag,
                description=""
            )
            GenerateBrandChunks.set_upstream([ExtractCatalogueRightWords, MergeMultiplePhraseKeys])

        CombinePhraseVariants = CoutureSparkOperator(
            task_id=vertical_prefix+"CombinePhraseVariants",
            dag=Dag,
            code_artifact=code_artifact,
            class_path=classPath,
            method_id="UnionTablesOnCommonCols",
            method_args_dict={
                "all_dfs": f"""{dirPathProcessed}BrandChunks,
                                {dirPathProcessed}BrandAcronyms,
                                {dirPathProcessed}PhrasesExternalVariants,
                                {dirPathProcessed}PhrasesPluralized,
                                {dirPathProcessed}PhrasesVariants,
                                {dirPathProcessed}InflectedPhrasesVariants""",
                "primary_cols": "phrase_key",
                "cols_handle_type": "intersect",
                "test": testing},
            input_base_dir_path="",
            output_base_dir_path=dirPathProcessed,
            input_filenames_dict={},
            output_filenames_dict={"union_df": "AllPhraseVariants"},
            # config_group="config_group_search_engine",
            description="Combines Phrase Variants from multiple sources"
        )
        CombinePhraseVariants.set_upstream([GeneratePhraseVariants])

        CombineTokenVariantsWithProperties = CoutureSparkOperator(
            task_id=vertical_prefix+"CombineTokenVariantsWithProperties",
            dag=Dag,
            code_artifact=code_artifact,
            class_path=classPath,
            method_id="CombineTokenVariantsWithProperties",
            method_args_dict={"test": "false"},
            input_base_dir_path=dirPathProcessed,
            output_base_dir_path=dirPathProcessed,
            input_filenames_dict={"all_word_variants": "W2RFinal1To1",
                                "word_properties": "RightwordsTranslated",
                                "all_phrase_variants": "AllPhraseVariants",
                                "phrase_properties": "PhrasesTranslated",
                                "additional_rightwords": "AdditionalRightwords",
                                "additional_phrases": "AdditionalPhrases"},
            output_filenames_dict={"word_variants_with_properties": "WordVariantsWithProperties",
                                "phrase_variants_with_properties": "PhraseVariantsWithProperties"
                                },
            # config_group="config_group_search_engine",
            description="Combines token properties with their variants"
        )
        CombineTokenVariantsWithProperties.set_upstream([CombinePhraseVariants, CombinePhraseProperties, CombineWordProperties])

        EnrichSplitterVocab = CouturePythonDockerOperator(
            task_id=vertical_prefix+"EnrichSplitterVocab",
            image=python_image,
            api_version="auto",
            auto_remove=False,
            user="couture",
            mem_limit="2g",
            extra_hosts=kerberos_hosts,
            dag=Dag,
            code_artifact=code_artifact_python_tejkiran,
            python_deps=[python_commons_egg, python_egg_phase2],
            method_id="enrich_splitter_vocab",
            method_args_dict={},
            input_base_dir_path=dirPathProcessed,
            output_base_dir_path=dirPathProcessed,
            input_filenames_dict={"rightwords_splitter_vocab": f"{dirPathProcessed}RightwordsCombined",
                                "accumulated_vocab": f"{dirCummulativePath}splitter_vocab/wordninja_words.txt.gz"},
            output_filenames_dict={},
            description=""
        )
        EnrichSplitterVocab.set_upstream([CombineTokenVariantsWithProperties])

        TrainContextSpellModel = CouturePythonDockerOperator(
            task_id=vertical_prefix+"TrainContextSpellModel",
            image=python_image,
            api_version='auto',
            auto_remove=False,
            mem_limit="18g",
            extra_hosts=kerberos_hosts,
            user='couture',
            dag=Dag,
            code_artifact=code_artifact_python_phase2,
            python_deps=[python_commons_egg, python_core_egg, python_egg_phase2, python_lumous_egg],
            method_id="context_spell_check_model_training",
            volumes=[searchEngineVolume],
            method_args_dict={
            "local_trained_model_path": "/home/pythondockervolume/ContextSpellTrainedModel",
            "local_tokenizer_path": "/home/pythondockervolume/tokenizer",
            "local_context_model_path": "/home/pythondockervolume/ContextSpellModel"
            },
            input_base_dir_path=dirPathProcessed,
            output_base_dir_path="/data1/searchengine/phase2/TrainedModels/jiomart/",
            input_filenames_dict={# "rightword_path": "AllRightWords",
                                "rightword_path": "RightwordsCombined",
                                "splitter_vocab_path": f"{dirCummulativePath}splitter_vocab/wordninja_words.txt.gz",
                                "w2r_exception_path": "WordVariantsWithPropertiesSelectColumnsForSpellCheckPkl",
                                # "w2r_exception_csr_path": "CategorySynonymsFiltered",
                                "context_model_path": "/data1/searchengine/phase2/LanguageModels/jiomart/context_model/jiomart_spell_checker_checkpoint_v2/",
                                "tokenizer_path": "/data1/searchengine/phase2/LanguageModels/jiomart/tokenizer/tokenizer-bert-base-jiomart-history-interaction-dict-fast-v2/",
                                },
            output_filenames_dict={"hdfs_model_path": "ContextSpellCheckTrainedModelLM"},
            description=""
        )
        TrainContextSpellModel.set_upstream([EnrichSplitterVocab])

        with TaskGroup(vertical_prefix+"GenerateCorpusJSONs", dag=Dag) as GenerateCorpusJSONs:
            GenerateInternalW2RJSON = CoutureSparkOperator(
                task_id=vertical_prefix+"GenerateInternalW2RJSON",
                dag=Dag,
                code_artifact=code_artifact,
                class_path=classPath,
                method_id="GenerateInternalW2RJSON",
                method_args_dict={"correct_word_column_name": "rightword",
                                "wrong_word_column_name": "wrongword"},
                input_base_dir_path=dirPathProcessed,
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={
                    # "wrong_to_right_words": "W2RPostProcessed"},
                    "wrong_to_right_words": "WordVariantsWithProperties"},
                output_filenames_dict={
                    "wrong_to_right_words_analysis_df": "analysis/Wrong2RightWordsAnalysisDF",
                    "wrong_to_right_words_analysis_json": "analysis/Wrong2RightWordsAnalysisJSON1",
                    "analysis_csv_r2x": "analysis/Wrong2RightWordsAnalysisCSVR2X",
                    "analysis_csv_w2r": "analysis/Wrong2RightWordsAnalysisCSVW2R",
                    "w2r": "analysis/W2RErr",
                    "r2cats": "analysis/R2Cats",
                    "r2otherprops": "analysis/R2OtherProps"},
                # config_group="config_group_search_engine_testing",
                description=""
            )
            # GenerateInternalW2RJSON.set_upstream([MakeManualChanges])
            GenerateInternalW2RJSON.set_upstream([CombineTokenVariantsWithProperties])

            GenerateInternalPhrasesJSON = CoutureSparkOperator(
                task_id=vertical_prefix+"GenerateInternalPhrasesJSON",
                dag=Dag,
                code_artifact=code_artifact,
                class_path=classPath,
                method_id="GenerateInternalPhrasesJSON",
                method_args_dict={"correct_word_column_name": "phrase",
                                "wrong_word_column_name": "phrase_key"},
                input_base_dir_path=dirPathProcessed,
                output_base_dir_path=dirPathProcessed,
        #         input_filenames_dict={"phrases": "PhrasesPostProcessed"},
                input_filenames_dict={"phrases": "PhraseVariantsWithProperties"},
                output_filenames_dict={"phrases_json": "JSONData/InternalPhraseCorpus",
                                    "phrases_csv": "JSONData/InternalPhraseCSV",
                                    "phrases_analysis_df": "analysis/PhrasesAnalysisJSON"},
                # config_group="config_group_search_engine_testing",
                description=""
            )
            # GenerateInternalPhrasesJSON.set_upstream([MakeManualChanges])
            GenerateInternalPhrasesJSON.set_upstream([CombineTokenVariantsWithProperties])

            EntityCorpusToJSONInternal = CoutureSparkOperator(
                task_id=vertical_prefix+"EntityCorpusToInternalJSON",
                dag=Dag,
                code_artifact=code_artifact,
                class_path=classPath,
                method_id="EntityCorpusToJSONInternal",
                method_args_dict={"pg_table_entity_corpus": "project_ajiowordentitiesdict"},
                input_base_dir_path=dirPathProcessed,
                output_base_dir_path=dirPathProcessed,
        #         input_filenames_dict={"entity_corpus": "EntityCorpusPostProcessed"},
                input_filenames_dict={"entity_corpus": "EntityCorpus"},
                output_filenames_dict={"entity_corpus_json_internal": "AnalysisCorpusJSON/EntityCorpusJSONInternal",
                                    "rightword_entities_csv": "JSONData/RightWordEntitiesCSV"},
                # config_group="config_group_search_engine",
                description=""
            )
            # EntityCorpusToJSONInternal.set_upstream([MakeManualChanges])
            EntityCorpusToJSONInternal.set_upstream([CombineTokenVariantsWithProperties])

            NumericalEntitiesToInternalJSON = CoutureSparkOperator(
                task_id=vertical_prefix+"NumericalEntitiesToInternalJSON",
                dag=Dag,
                code_artifact=code_artifact,
                class_path=classPath,
                method_id="NumericalEntitiesToJson",
                method_args_dict={"pg_table_numerical_corpus": "project_ajionumericalrangedict"},
                input_base_dir_path=dirPathProcessed,
                output_base_dir_path=dirPathProcessed,
                input_filenames_dict={"numerical_entities_range_wise": "NumericalEntitiesRangeWise"},
                output_filenames_dict={"numerical_entities_json": "JSONData/NumericalEntitiesJSONInternal",
                                    "numerical_entities_csv": "JSONData/NumericalEntitiesCSV"},
                config_group="config_group_search_engine",
                description="Converts numeric range data to format required by analysis pipeline and simulator"
            )
            NumericalEntitiesToInternalJSON.set_upstream([PopulateNumericalEntitiesRangeWise])
            
        # === Update Simulator === #
        bash_cmd_update_simulator = """
        cd /data/searchengine && bash update_simulator.sh {} {} {}
        """.format(catalogue_date, environment[Dag.dag_id], versions[Dag.dag_id])
        UpdateSimulator = SSHOperator(
            ssh_conn_id="New121_CONN_SSH_SERVER",  # To connect to HD3
            task_id=vertical_prefix+"UpdateSimulator",
            command=bash_cmd_update_simulator,  # Runs these commands on the SSH"ed server (i.e. DBS)
            # description="",
            dag=Dag
        )
        UpdateSimulator.set_upstream(
            [GenerateInternalPhrasesJSON, GenerateInternalW2RJSON, NumericalEntitiesToInternalJSON,
            EntityCorpusToJSONInternal])


        return wrappingGroup