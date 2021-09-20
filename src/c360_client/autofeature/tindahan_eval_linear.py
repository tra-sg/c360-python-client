import os
import pandas as pd
import numpy as np
from sklearn.metrics import roc_auc_score


def run():
    backtest_dates = [
        "2021-03-01",
        "2021-04-01",
        "2021-05-01",
        "2021-06-01",
    ]

    for start_date in backtest_dates:
        # model_archive_prefix = os.path.join(
        #     "ftl_xgb_result", start_date,
        # )
        model_archive_prefix = os.path.join(
            "ftl_xgb_result", "linear", start_date
        )

        result_df = pd.read_csv(
            os.path.join(model_archive_prefix, "result.csv")
        )

        print(start_date, "AUC", roc_auc_score(result_df["label"], result_df["pred_label"]))

        cutoff = 0.5
        # result_df["acc"] = (result_df["label"] == result_df["pred_label"])
        result_df["acc"] = (
            result_df["label"] == np.where(result_df["pred_label"] > cutoff, 1, 0)
        )
        print(
            start_date, "predicted", (np.where(result_df["pred_label"] > cutoff, 1, 0)).sum(),
            "out of", result_df.shape[0]
        )
        print(
            start_date, "actual", (result_df["label"]).sum(),
            "out of", result_df.shape[0]
        )

        acc = result_df["acc"].sum() / result_df.shape[0]

        result_df_tpr = result_df[result_df["label"] == 1]
        tpr = result_df_tpr["pred_label"].sum() / result_df_tpr.shape[0]

        result_df_fpr = result_df[result_df["label"] == 0]
        fpr = result_df_fpr["pred_label"].sum() / result_df_fpr.shape[0]

        print(start_date, "accuracy", acc)
        print(start_date, "tpr", tpr)
        print(start_date, "fpr", fpr)

        # print(result_df.head())


if __name__ == "__main__":
    run()
