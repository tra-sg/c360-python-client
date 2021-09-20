"""
Source: https://medium.com/feature-labs-engineering/scaling-featuretools-with-dask-ce46f9774c7d
"""

import pandas as pd
import numpy as np

# featuretools for automated feature engineering
import featuretools as ft
import featuretools.variable_types as vtypes

# Utilities
import sys
import psutil
import os

from timeit import default_timer as timer

import math

PARTITION_PATH_PREFIX = "ftparallel"


def convert_types(df):
    """Convert pandas data types for memory reduction."""

    # Iterate through each column
    for c in df:

        # Convert ids and booleans to integers
        if ('SK_ID' in c):
            df[c] = df[c].fillna(0).astype(np.int32)

        # Convert objects to category
        elif (df[c].dtype == 'object') and (df[c].nunique() < df.shape[0]):
            df[c] = df[c].astype('category')

        # Booleans mapped to integers
        elif set(df[c].unique()) == {0, 1}:
            df[c] = df[c].astype(bool)

        # Float64 to float32
        elif df[c].dtype == float:
            df[c] = df[c].astype(np.float32)

        # Int64 to int32
        elif df[c].dtype == int:
            df[c] = df[c].astype(np.int32)

    return df


def partition_entity_set(es, name, target_entity="customers", batch=6*9, with_index=False):
    main_df = es[target_entity].df
    batch_size = math.ceil(main_df.shape[0] / batch)

    output_prefix = os.path.join(PARTITION_PATH_PREFIX, name)

    for b in range(batch):
        print("Partitioning batch", b, "of", batch - 1)

        batch_prefix = os.path.join(output_prefix, f"p{b}")
        os.makedirs(batch_prefix, exist_ok=True)

        batch_index = (
            b*batch_size,
            (b+1) * batch_size
        )
        main_df_piece = main_df.iloc[batch_index[0]:batch_index[1]]
        main_df_piece.to_csv(
            os.path.join(batch_prefix, f"{target_entity}.csv"), index=with_index,
        )

        main_index_name = es[target_entity].index
        main_piece_index = main_df_piece[main_index_name]

        for entity_name in es.entity_dict:
            if entity_name == target_entity:
                continue

            entity_df = es[entity_name].df

            if es[target_entity].index in entity_df.columns:
                related_var = get_related_var(es, target_entity, entity_name, main_index_name)
                entity_df_piece = entity_df[entity_df[related_var].isin(main_piece_index)]
            else:
                entity_df_piece = entity_df

            entity_df_piece.to_csv(
                os.path.join(batch_prefix, f"{entity_name}.csv"), index=with_index,
            )


def get_related_var(es, entity1, entity2, var1):
    """
    Given 2 entities and the variable of the first, get the second variable
    that is related.
    """
    for reln in es.relationships:
        rel = reln.to_dictionary().values()
        if entity1 in rel:
            if entity2 in rel:
                # return rel[entity2]
                if var1 in rel:
                    possible_var = [
                        i for i in rel
                        if i not in (entity1, entity2, var1)
                    ]

                    if len(possible_var) == 0:
                        return var1
                    else:
                        return possible_var[0]

    raise ValueError("Relationship not found:", entity1, entity2, var1)


def feature_matrix_from_entity(es, name, part, with_index=False, **kwargs):
    features, feature_names = ft.dfs(entityset=es, **kwargs)
    feature_matrix_enc, features_enc = ft.encode_features(features, feature_names)
    feature_matrix_enc = convert_types(feature_matrix_enc)
    os.makedirs(os.path.dirname(get_output_path(name, part)), exist_ok=True)
    feature_matrix_enc.to_csv(get_output_path(name, part), index=with_index)

    return feature_matrix_enc


def get_partition_path(name, part, entity):
    return os.path.join(PARTITION_PATH_PREFIX, name, f"p{part}", f"{entity}.csv")


def get_output_path(name, part):
    return os.path.join(PARTITION_PATH_PREFIX, name, "output", f"{part}.csv")
