import os
import featuretools
import pandas as pd
import numpy as np
from c360_client.autofeature import (
    PARTITION_PATH_PREFIX,
    feature_matrix_from_entity,
    get_partition_path,
    convert_types,
)
from timeit import default_timer as timer

import dask.bag as db
from dask.distributed import Client


SET_NAME = "ulph_tindahan"
GROWTH_THRESHOLD = 1.3


def load_es_from_partition(part):
    es4 = featuretools.EntitySet(id="customers-month")

    order_df = pd.read_csv(get_partition_path(SET_NAME, part, "order_item"))
    product_sku_df = pd.read_csv(get_partition_path(SET_NAME, part, "products"))

    es4 = es4.entity_from_dataframe(
        entity_id="order_item",
        dataframe=order_df,
        # make_index=True,
        index="order_item_id",
        time_index="so_date"
    )
    es4 = es4.normalize_entity(
        new_entity_id="customer",
        base_entity_id="order_item",
        index="outlet",
        # additional_variables=["region", "area", "distributor_name", "outlet_name", "master_outlet_subtype", "master_outlet_subtype_name", "site_name"]
        additional_variables=["region", "area", "distributor_name", "master_outlet_subtype_name", "site_name"]
    )
    es4 = es4.normalize_entity(
        new_entity_id="order",
        base_entity_id="order_item",
        index="so_document",
        make_time_index=True,
        # additional_variables=["so_date"],
    )

    es4 = es4.entity_from_dataframe(entity_id="products", dataframe=product_sku_df, index="sku_name")

    es4 = es4.add_relationship(
        featuretools.Relationship(
            es4["products"]["sku_name"],
            es4["order_item"]["sku_name"]
        )
    )

    # add interesting values
    # es4["customers"]["subtype"].interesting_values = ["SARI-SARI STORE BIG", "SARI-SARI STORE MEDI", "SARI-SARI STORE SMAL"]
    es4["order_item"]["brand"].interesting_values = ["SURF", "KNORR", "CREAM SILK", "SUNSILK", "DOVE WOMEN", "LADY'S CHOICE", "BREEZE", "CLOSE UP"]
    es4["products"]["promo_freq"].interesting_values = ["Regular", "Promo"]
    es4["products"]["packgrp_unit"].interesting_values = ["ML", "L", "G"]

    return es4


# def get_cutoff_df(t200_order_df):
#     t200_order_df["month"] = t200_order_df["so_date"].to_numpy().astype('datetime64[M]')
#     t200_order_agg = t200_order_df.groupby(["outlet", "month"]).agg({"ordered_amount": "sum"})\
#         .reset_index().sort_values(by=["outlet", "month"])
#     t200_order_agg["next_month"] = t200_order_agg.groupby('outlet')['month'].shift(-1)
#     t200_order_agg["next_rev"] = t200_order_agg.groupby('outlet')['ordered_amount'].shift(-1)
#     t200_order_agg["growth"] = np.where(
#         t200_order_agg["ordered_amount"] > 0,
#         t200_order_agg["next_rev"] / t200_order_agg["ordered_amount"],
#         np.nan
#     )

#     # t200_order_agg = t200_order_agg[t200_order_agg["ordered_amount"] >= 100]
#     t200_order_agg = t200_order_agg[
#         t200_order_agg["ordered_amount"].between(
#             t200_order_agg["ordered_amount"].quantile(.05),
#             t200_order_agg["ordered_amount"].quantile(.95)
#         )
#     ]
#     print("order amount distribution")
#     print(t200_order_agg["ordered_amount"].describe())

#     t200_order_agg["label"] = np.where(t200_order_agg["growth"] > GROWTH_THRESHOLD, 1, 0)
#     t200_order_cutoff = t200_order_agg[["outlet", "month", "label"]]
#     t200_order_cutoff.columns = ["outlet", "time", "label"]
#     return t200_order_cutoff


def get_cutoff_df(t200_order_df):
    """
    There is a 30% growth to 30% which is perhaps too big.
    Here we try to capture a growth that isn't dropping afterwards.
    """
    t200_order_df["month"] = t200_order_df["so_date"].to_numpy().astype('datetime64[M]')
    t200_order_agg = t200_order_df.groupby(["outlet", "month"]).agg({"ordered_amount": "sum"})\
        .reset_index().sort_values(by=["outlet", "month"])
    t200_order_agg["next_month"] = t200_order_agg.groupby('outlet')['month'].shift(-1)
    t200_order_agg["prev_rev"] = t200_order_agg.groupby('outlet')['ordered_amount'].shift(1)
    t200_order_agg["next_rev"] = t200_order_agg.groupby('outlet')['ordered_amount'].shift(-1).fillna(0)
    t200_order_agg["next2_rev"] = t200_order_agg.groupby('outlet')['ordered_amount'].shift(-2).fillna(0)
    t200_order_agg["growth"] = np.where(
        t200_order_agg["ordered_amount"] > 0,
        t200_order_agg["next_rev"] / t200_order_agg["ordered_amount"],
        np.nan
    )
    t200_order_agg["nex_growth"] = np.where(
        t200_order_agg["next_rev"] > 0,
        t200_order_agg["next2_rev"] / t200_order_agg["next_rev"],
        np.nan
    )
    t200_order_agg["prev_growth"] = np.where(
        t200_order_agg["prev_rev"] > 0,
        t200_order_agg["ordered_amount"] / t200_order_agg["prev_rev"],
        np.nan
    )

    # t200_order_agg = t200_order_agg[t200_order_agg["ordered_amount"] >= 100]
    t200_order_agg = t200_order_agg[
        t200_order_agg["ordered_amount"].between(
            t200_order_agg["ordered_amount"].quantile(.05),
            t200_order_agg["ordered_amount"].quantile(.95)
        )
    ]
    print("order amount distribution")
    print(t200_order_agg["ordered_amount"].describe())

    t200_order_agg["label"] = np.where(
        # (t200_order_agg["growth"] > GROWTH_THRESHOLD) & (t200_order_agg["nex_growth"] > 1),
        (t200_order_agg["growth"] > GROWTH_THRESHOLD) & (t200_order_agg["prev_growth"] > 0.8),
        1, 0,
    )
    t200_order_cutoff = t200_order_agg[["outlet", "month", "label"]]
    t200_order_cutoff.columns = ["outlet", "time", "label"]
    return t200_order_cutoff


# def get_cutoff_df(t200_order_df):
#     """
#     Cutoff DF for current month being high growth (rather than next month)
#     """
#     t200_order_df["month"] = t200_order_df["so_date"].to_numpy().astype('datetime64[M]')
#     t200_order_agg = t200_order_df.groupby(["outlet", "month"]).agg({"ordered_amount": "sum"})\
#         .reset_index().sort_values(by=["outlet", "month"])
#     t200_order_agg["next_month"] = t200_order_agg.groupby('outlet')['month'].shift(1)
#     t200_order_agg["prev_rev"] = t200_order_agg.groupby('outlet')['ordered_amount'].shift(1)
#     t200_order_agg = t200_order_agg[~t200_order_agg["prev_rev"].isna()]
#     t200_order_agg["growth"] = np.where(
#         t200_order_agg["prev_rev"] > 0,
#         t200_order_agg["ordered_amount"] / t200_order_agg["prev_rev"],
#         np.nan
#     )
#     print("order amount distribution")
#     print(t200_order_agg["ordered_amount"].describe())
#     t200_order_agg["label"] = np.where(t200_order_agg["growth"] > GROWTH_THRESHOLD, 1, 0)
#     t200_order_cutoff = t200_order_agg[["outlet", "month", "label"]]
#     t200_order_cutoff.columns = ["outlet", "time", "label"]
#     return t200_order_cutoff


def generate_feature_for_part(part):
    es = load_es_from_partition(part)

    t200_order_cutoff = get_cutoff_df(es["order_item"].df)
    # print(t200_order_cutoff)
    if t200_order_cutoff.shape[0] <= 0:
        print(f"part {part} has zero rows, skipping")
        return

    feature_matrix_from_entity(
        es, SET_NAME, part, with_index=True,
        target_entity="customer",
        agg_primitives=["mean", "max", "min", "sum", "std", "skew", "num_unique", "count"],
        # trans_primitives=["is_null", "percentile"],
        trans_primitives=["is_null"],
        cutoff_time=t200_order_cutoff,
        cutoff_time_in_index=True,
        chunk_size=len(t200_order_cutoff),
        max_depth=1,
    )
    # print(feature_matrix_enc.head())


if __name__ == "__main__":

    # test
    # es = load_es_from_partition(1)
    # print(es)
    # for i in range(6*9):
    #     generate_feature_for_part(i)

    # Use all 8 cores
    client = Client(processes=True)

    b = db.from_sequence(range(6*9))
    b = b.map(generate_feature_for_part)

    overall_start = timer()
    b.compute()
    overall_end = timer()

    print(f"Total Time Elapsed: {round(overall_end - overall_start, 2)} seconds.")
