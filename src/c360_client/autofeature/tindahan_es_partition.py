import featuretools
import pandas as pd
from c360_client.autofeature import partition_entity_set


SET_NAME = "ulph_tindahan"


def get_valid_package(series):
    candidates = list(series.unique())
    if len(candidates) == 0:
        return "NOT IN LIST"
    if len(candidates) == 1:
        return candidates[0]
    else:
        candidates.remove("NOT IN LIST")
        return candidates[0]


if __name__ == "__main__":
    # get df

    print("loading data")
    t200_order = pd.read_csv("t200_order.csv")
    # t200_order = pd.read_csv("t200_order.csv", nrows=1000)  # trial

    product_df = pd.read_csv("product_sku.csv")
    product_sku_df = product_df[["fe_big_c", "fe_small_c", "fe_brand", "fe_package", "fe_packgroup", "fe_promo_regular", "material_description"]]
    product_sku_df.columns = ["category", "subcategory", "brand", "package", "packgroup", "promo_freq", "sku_name"]
    product_sku_df["package"] = product_sku_df["package"].str.upper()
    product_sku_df = product_sku_df.drop_duplicates()

    product_sku_df = product_sku_df.groupby("sku_name").agg({
        "category": "max",
        "subcategory": "max",
        "brand": "max",
        "package": get_valid_package,
        "packgroup": "max",
        "promo_freq": "max",
    }).reset_index()

    product_sku_df["packgrp_size"] = product_sku_df["packgroup"].str.split("/").str[0].str.extract('(\d+)')
    product_sku_df["packgrp_unit"] = product_sku_df["packgroup"].str.split("/").str[0].str.extract('([a-zA-Z]+)')

    print("constructing EntitySet to partition")
    es4 = featuretools.EntitySet(id="customers-month")
    es4 = es4.entity_from_dataframe(
        entity_id="order_item",
        # dataframe=t200_order_df_,
        dataframe=t200_order,
        make_index=True,
        index="order_item_id",
        time_index="so_date"
    )
    es4 = es4.normalize_entity(
        new_entity_id="customer",
        base_entity_id="order_item",
        index="outlet",
    )
    # es4 = es4.normalize_entity(
    #     new_entity_id="customer",
    #     base_entity_id="order_item",
    #     index="outlet",
    #     additional_variables=[
    #         "region", "area", "distributor_name", "outlet_name",
    #         "master_outlet_subtype", "master_outlet_subtype_name", "site_name"
    #     ]
    # )
    # es4 = es4.normalize_entity(
    #     new_entity_id="order",
    #     base_entity_id="order_item",
    #     index="so_document",
    #     make_time_index=True,
    # )

    es4 = es4.entity_from_dataframe(
        entity_id="products", dataframe=product_sku_df, index="sku_name"
    )

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

    print("partitioning..")
    partition_entity_set(es4, SET_NAME, target_entity="customer")
