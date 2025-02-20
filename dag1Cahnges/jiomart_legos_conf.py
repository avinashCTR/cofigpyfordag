task_configs = {
  "ExtractCatalogueRightWords": {
    "method_args_dict": {
      "max_rightword_length": 17,
      "max_categories": 1000,
      "min_category_percentile": 1,
      "min_category_wise_rightword_percentage": 1,
      "min_product_count": 30,
      "min_product_count_for_known_words": 500
    }
  },
  "ExtractPhrasesFromSpecificAttributes": {
    "method_args_dict": {
      "min_product_count": "5",
      "next_best_fraction": 0.5,
      "test": "testing"
    }
  },
  "GenerateDerivedPhrases": {
    "method_args_dict": {
      "min_entities_percentile": 0.9973
    }
  },
  "GetBrandFamilySynonyms": {
    "method_args_dict": {
      "min_count_for_brand_significance": 10
    }
  },
  "GenerateSimilarWords": {
    "method_args_dict": {
      "user_column_name": "rightword",
      "feature_column_name": "vector",
      "max_similar_items": "1",
      "similar_hierarchical": "false",
      "similar_algo": "dotcosine"
    }
  },
  "SplitWords": {
    "method_args_dict": {
      "splitword_products_threshold": 20,
      "single_character_products_threshold": 1000,
      "test": "testing"
    }
  },
  "FilterRightQueriesFromHistory": {
    "method_args_dict": {
      "query_col": "query",
      "query_split_col": "query_normalised",
      "freq_col": "freq",
      "min_query_clicks_confidence": 5
    }
  },
  "FilterCategoriesForPhrases": {
    "method_args_dict": {
      "correct_word_column_name": "phrase"
    }
  },
  "FilterCategoriesForWords": {
    "method_args_dict": {
      "correct_word_column_name": "rightword"
    }
  },
  "FilterExternalVariants": {
    "method_args_dict": {
      "external_word_column": "word"
    }
  },
  "GeneratePhoneticVariants": {
    "method_args_dict": {
      "phonetic_edit_dist_threshold": 0.85
    }
  },
  "GenerateIPATransliterations": {
    "method_args_dict": {
      "max_ipa_computations": 15000,
      "test": "true"
    }
  },
  "CalculateDamerauLevenshteinSimilarity": {
    "method_args_dict": {
      "compare_columns_list": "wrongword,rightword,ipa_wrongword,ipa_rightword",
      "score_columns_list": "levenshtein_score,phonetic_score"
    }
  },
  "GetW2RScoreFromHistory": {
    "method_args_dict": {
      "query_column": "query",
      "history_freq_column": "freq",
      "product_id_history": "sku",
      "min_token_count": 0,
      "max_query_length": 3,
      "analysis": "false"
    }
  },
  "CalculateFinalScore": {
    "method_args_dict": {
      "levenshtein_weight": 0.25,
      "phonetics_weight": 0.25,
      "history_precision_weight": 0.5
    }
  }
}
