import pandas as pd
import numpy as np
import pycaret.classification as pc
import matplotlib.pyplot as plt
from timeit import default_timer as timer


def get_order_df():
    # t200_order_df = pd.read_csv("t200_order.csv")
    t200_order_df = pd.read_csv("t200_order_nofilter.csv")
    t200_order_df = t200_order_df[t200_order_df["so_date"] >= "2021-01-01"]
    t200_order_df["month"] = t200_order_df["so_date"].to_numpy().astype('datetime64[M]')
    t200_order_agg = t200_order_df.groupby(["outlet", "month"]).agg({'ordered_amount': 'sum'})\
        .reset_index().sort_values(by=["outlet", "month"])
    t200_order_agg["ori_ordered_amount"] = t200_order_agg["ordered_amount"]
    t200_order_agg["ordered_amount"] = t200_order_agg.groupby("outlet")["ori_ordered_amount"].rolling(3).mean().reset_index(0, drop=True)
    t200_order_agg["next_rev"] = t200_order_agg.groupby('outlet')["ordered_amount"].shift(-1)
    t200_order_agg["growth"] = np.where(
        (t200_order_agg["ordered_amount"] > 0),
        t200_order_agg["next_rev"] / t200_order_agg["ordered_amount"],
        np.nan
    )
    t200_order_agg["label"] = np.where(t200_order_agg["growth"] > 1.6, 1, 0)
    t200_order_wlabel = t200_order_df.merge(t200_order_agg[["outlet", "month", "label"]], on=["outlet", "month"], how="left")
    # t200_order_wlabel = t200_order_wlabel.replace({pd.NA: np.nan})
    t200_order_wlabel["ordered_unit"] = t200_order_wlabel["pack_size"] * t200_order_wlabel["sales_order_qty_cs"]
    # t200_order_wlabel["invoiced_unit"] = t200_order_wlabel["pack_size"] * t200_order_wlabel["invoiced_qty_in_cs"]
    t200_order_wlabel["price"] = t200_order_wlabel["ordered_amount"] / t200_order_wlabel["ordered_unit"]

    # cant even run full data locally (says 350 gb required)
    # frac 0.02 killed
    t200_order_wlabel_sample = t200_order_wlabel[
            t200_order_wlabel["outlet"].isin(t200_order_agg.sample(frac=0.05)["outlet"])
            # t200_order_wlabel["outlet"].isin(t200_order_agg.sample(n=200)["outlet"])
    ]

    # 80 / 20 train test split
    t200_order_agg_pop = t200_order_wlabel_sample[["outlet", "month"]].drop_duplicates()
    print(t200_order_agg_pop.shape)
    t200_customer_train = t200_order_agg_pop.groupby("month").sample(frac=0.8)
    print(t200_customer_train.shape)
    t200_order_agg_wsplit = t200_order_agg_pop.merge(
        t200_customer_train[["outlet", "month"]], on=["outlet", "month"], how="left", indicator=True,
    )
    print(t200_order_agg_wsplit.shape)
    t200_customer_test = t200_order_agg_wsplit[
        t200_order_agg_wsplit["_merge"] == "left_only"
    ][["outlet", "month"]]

    t200_order_wlabel_sample_train = t200_order_wlabel_sample.merge(
        t200_customer_train[["outlet", "month"]], on=["outlet", "month"], how="inner",
    )
    t200_order_wlabel_sample_test = t200_order_wlabel_sample.merge(
        t200_customer_test[["outlet", "month"]], on=["outlet", "month"], how="inner",
    )

    t200_order_wlabel_sample_train.to_csv("pcrt_rf_train.csv", index=False)
    t200_order_wlabel_sample_test.to_csv("pcrt_rf_test.csv", index=False)
    print(f"""Data ({t200_order_wlabel_sample.shape}):
    - Train ({t200_order_wlabel_sample_train.shape})
    - Test ({t200_order_wlabel_sample_test.shape})
    """)

    # return t200_order_wlabel
    return t200_order_wlabel_sample_train, t200_order_wlabel_sample_test


def run_pycaret(order_df, test_order_df):
    exp = pc.setup(
        # data=order_df.drop(["sku", "sku_name", "outlet", "outlet_name", "site_name", "distributor_name"], axis=1),
        data=order_df,
        test_data=test_order_df,
        ignore_features=[
            "sku_name", "outlet", "site_name", "distributor_name",
            "month", "so_document",  # "online_portion",
            "sku", "distributor", "master_outlet_subtype", "site", "outlet_name",
        ],
        target="label",
        session_id=123,
        normalize=True,
        transformation=True,
        ignore_low_variance=True,
        remove_multicollinearity=True,
        multicollinearity_threshold=0.95,
        feature_interaction=False,  # setting this to True will crash notebooks
        silent=True,
    )

    model = pc.create_model('rf', fold=5)
    # model = pc.create_model('lightgbm', fold=5)
    tuned_model = pc.tune_model(model, optimize='AUC')

    pc.plot_model(tuned_model, save=True)
    pc.plot_model(tuned_model, 'feature_all', save=True)
    pc.plot_model(tuned_model, 'confusion_matrix', save=True)

    prediction_df = pc.predict_model(tuned_model)[["Label", "Score"]]
    prediction_df.columns = ["pred_label", "model_score"]

    test_order_df_wlabel = pd.concat([test_order_df, prediction_df], axis=1)
    test_order_df_wlabel.to_csv("pcrt_test_result.csv", index=False)


def run():
    order_df, test_order_df = get_order_df()

    overall_start = timer()
    run_pycaret(order_df, test_order_df)
    overall_end = timer()
    print(f"Total Time Elapsed: {round(overall_end - overall_start, 2)} seconds.")


if __name__ == '__main__':
    run()
