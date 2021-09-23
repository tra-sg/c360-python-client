import pandas as pd
import numpy as np


def run():
    eval_df = pd.read_csv("pcrt_test_result.csv")

    eval_df_agg = eval_df.groupby(["outlet", "month"]).agg({
        "so_document": pd.Series.nunique,
        "label": "mean",
        "pred_label": "mean",
    }).reset_index()
    eval_df_agg["combine_pred"] = np.where(eval_df_agg["pred_label"] > 0.5, 1, 0)
    eval_df_agg["combine_pred_highthresh"] = np.where(eval_df_agg["pred_label"] > 0.75, 1, 0)
    eval_df_agg["truepos"] = ((eval_df_agg["combine_pred"] == 1) & (eval_df_agg["label"] == 1))
    print(eval_df_agg.head())

    pop = eval_df_agg.shape[0]
    true_ = (eval_df_agg["combine_pred"] == eval_df_agg["label"]).sum()
    true_pos = eval_df_agg["truepos"].sum()

    print("Acc =", true_, '/', pop, '=', true_ / pop)
    print("True Positive =", true_pos)

    # eval_df_magg = eval_df_agg.groupby(["month", "label"]).agg({
    eval_df_magg = eval_df_agg.groupby(["month"]).agg({
        "outlet": pd.Series.nunique,
        "label": "sum",
        "combine_pred": "sum",
        "combine_pred_highthresh": "sum",
        "truepos": "sum",
    })
    print(eval_df_magg)


if __name__ == "__main__":
    run()
