import pandas as pd
import xgboost as xgb
from datetime import datetime
from c360_client.autofeature.tindahan_train import (
    get_all_feature_matrix,
    train_xgboost_w_featurematrix,
)
import os
import matplotlib.pyplot as plt
import featuretools as ft
from featuretools.selection import remove_highly_correlated_features, remove_highly_null_features
import shap
import numpy as np

SET_NAME = "ulph_tindahan"


def run_with_backtesting():
    print("Training with backtesting")
    feature_matrix = get_all_feature_matrix()
    # print("Label count:", feature_matrix.groupby("label").agg({"outlet": "count"}))
    # feature_matrix = feature_matrix*1
    # convert_columns = [i for i in feature_matrix.columns if i not in ("outlet", "time")]
    # feature_matrix[convert_columns] = feature_matrix[convert_columns]\
    #     .apply(pd.to_numeric, errors='coerce', axis=1).fillna(0)

    # new step
    feature_names = ft.load_features("ftparallel/ulph_tindahan/output/0.json")
    feature_matrix, features_enc = ft.encode_features(feature_matrix, feature_names)
    feature_matrix = remove_highly_correlated_features(feature_matrix)
    feature_matrix = remove_highly_null_features(feature_matrix)
    feature_matrix = (feature_matrix*1).fillna(0)

    # handling inf
    feature_matrix = feature_matrix.replace([np.inf, -np.inf], np.nan).dropna()

    # print("Label count:", feature_matrix.groupby("label").agg({"outlet": "count"}))

    # raise Exception()

    backtest_windows = [
        ("2021-03-01", "2021-04-01"),
        ("2021-04-01", "2021-05-01"),
        ("2021-05-01", "2021-06-01"),
        ("2021-06-01", "2021-07-01"),
    ]

    for start_date, end_date in backtest_windows:
        print("Training", start_date, end_date)
        train_df = feature_matrix[feature_matrix["time"] <= start_date]
        test_df = feature_matrix[feature_matrix["time"] == end_date]

        model = train_xgboost_w_featurematrix(train_df)

        # model_archive_prefix = os.path.join(
        #     "ftl_xgb_result", start_date,
        # )
        model_archive_prefix = os.path.join(
            "ftl_xgb_result", "linear", start_date,
        )
        os.makedirs(model_archive_prefix, exist_ok=True)

        model.save_model(
            os.path.join(model_archive_prefix, "mdl.model")
        )

        plt.tight_layout()
        xgb.plot_importance(model, max_num_features=40)
        plt.savefig(
            os.path.join(model_archive_prefix, "importance.png")
        )

        explainer = shap.Explainer(model)

        columns_todrop = [
            # "outlet", "time", "label",
            "so_document", "time", "label",
            *[i for i in train_df.columns if "order_item.so_document)" in i],
            *[i for i in train_df.columns if "order_item.sku)" in i],
            *[i for i in train_df.columns if "order_item.distributor)" in i],
            *[i for i in train_df.columns if "order_item.online_portion)" in i],
        ]

        train_x = train_df.drop(columns_todrop, axis=1)
        shap_values = explainer(train_x)

        plt.figure(figsize=(20, 12))
        shap.plots.waterfall(shap_values[0], show=False, max_display=20)
        plt.savefig(
            os.path.join(model_archive_prefix, "shap_wf.png")
        )

        plt.figure(figsize=(20, 12))
        shap.plots.beeswarm(shap_values, show=False, max_display=20)
        plt.savefig(
            os.path.join(model_archive_prefix, "shap_bs.png")
        )

        feature_names_stripped = [
            f for f in test_df.columns
            if f not in columns_todrop
        ]
        test_df_feature = test_df[feature_names_stripped]
        test_df["pred_label"] = model.predict(test_df_feature)

        # test_df_res = test_df[["outlet", "time", "label", "pred_label"]]
        # test_df_res.to_csv(
        #     os.path.join(model_archive_prefix, "result.csv"), index=False
        # )

        test_df_res = test_df[["so_document", "time", "label", "pred_label"]]
        test_df_res.to_csv(
            os.path.join(model_archive_prefix, "result.csv"), index=False
        )


if __name__ == "__main__":
    run_with_backtesting()
