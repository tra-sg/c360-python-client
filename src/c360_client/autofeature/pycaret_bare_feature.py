import pandas as pd
import numpy as np
import pycaret.classification as pc
import matplotlib.pyplot as plt
from timeit import default_timer as timer


def get_order_df():
    t200_order_df = pd.read_csv("t200_order.csv")
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
            t200_order_wlabel["outlet"].isin(t200_order_agg.sample(frac=0.1)["outlet"])
            # t200_order_wlabel["outlet"].isin(t200_order_agg.sample(n=10)["outlet"])
    ]

    # return t200_order_wlabel
    return t200_order_wlabel_sample


def run_pycaret(order_df):
    exp = pc.setup(
        # data=order_df.drop(["sku", "sku_name", "outlet", "outlet_name", "site_name", "distributor_name"], axis=1),
        data=order_df.drop([
            "sku_name", "outlet", "site_name", "distributor_name",
            "month", "so_document", "online_portion",
            # "sku", "distributor", "master_outlet_subtype", "site",
        ], axis=1),
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
    best_model = pc.compare_models(sort="AUC")
    print(best_model)
    grid = pc.pull()
    grid.to_csv("pcrt_model_comparison.csv", index=False)
    print(grid)

    lgbm = pc.create_model('lightgbm', fold=5)
    tuned_lgbm = pc.tune_model(lgbm, optimize='AUC')

    # plt.figure()
    pc.plot_model(tuned_lgbm, save=True)
    # plt.savefig("pycaret_bare_auc.png")

    # plt.figure()
    pc.plot_model(tuned_lgbm, 'feature_all', save=True)
    # plt.savefig("pycaret_bare_importance.png")

    pc.plot_model(tuned_lgbm, 'confusion_matrix', save=True)


def run():
    order_df = get_order_df()

    overall_start = timer()
    run_pycaret(order_df)
    overall_end = timer()
    print(f"Total Time Elapsed: {round(overall_end - overall_start, 2)} seconds.")


if __name__ == '__main__':
    print("Starting pycaret bare")
    run()
    print("Finished pycaret bare")
