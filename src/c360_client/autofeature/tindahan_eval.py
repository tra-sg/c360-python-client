import os
import pandas as pd


def run():
    backtest_dates = [
        "2021-03-01",
        "2021-04-01",
        "2021-05-01",
        "2021-06-01",
    ]

    for start_date in backtest_dates:
        model_archive_prefix = os.path.join("ftl_xgb_result", start_date)

        result_df = pd.read_csv(
            os.path.join(model_archive_prefix, "result.csv")
        )
        result_df["acc"] = (result_df["label"] == result_df["pred_label"])

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
