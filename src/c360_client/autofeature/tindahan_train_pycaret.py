import pandas as pd
from datetime import datetime
from c360_client.autofeature import get_output_path, convert_types
from matplotlib import rcParams
import matplotlib.pyplot as plt

# import pycaret.classification as pc
# from pycaret.utils import enable_colab as pycaret_enable_colab

from autoviml.Auto_ViML import Auto_ViML
import shap
import os

SET_NAME = "ulph_tindahan"

rcParams.update({'figure.autolayout': True, 'figure.figsize': (20, 10)})


def get_all_feature_matrix():
    tables = []

    for i in range(6*9):
    # for i in range(1):  # test
        try:
            piece_df = pd.read_csv(get_output_path(SET_NAME, i)).dropna()
            tables.append(convert_types(piece_df))
        except FileNotFoundError:
            continue

    return pd.concat(tables)


def train_pycaret_w_featurematrix(fm):

    columns_todrop = [
        "outlet", "time",
        *[i for i in fm.columns if "order_item.so_document)" in i],
        *[i for i in fm.columns if "order_item.sku)" in i],
        *[i for i in fm.columns if "order_item.distributor)" in i],
        *[i for i in fm.columns if "order_item.online_portion)" in i],
    ]

    exp = pc.setup(
        data=fm.drop(columns_todrop, axis=1),
        target="label",
        session_id=123,
    )

    best_model = pc.compare_models(sort="AUC")
    print(pc.pull())
    print(best_model)


def use_auto_viml(fm):

    columns_todrop = [
        "outlet", "time",
        *[i for i in fm.columns if "order_item.so_document)" in i],
        *[i for i in fm.columns if "order_item.sku)" in i],
        *[i for i in fm.columns if "order_item.distributor)" in i],
        *[i for i in fm.columns if "order_item.online_portion)" in i],
    ]

    model, features, trainm, testm = Auto_ViML(
        train=fm.drop(columns_todrop, axis=1),
        target="label",
        hyper_param="RS",
        feature_reduction=True,
        scoring_parameter="balanced_accuracy",
        Boosting_Flag=True,
        verbose=2,
    )

    print(features)
    explainer = shap.Explainer(model)

    train_x = trainm
    shap_values = explainer(train_x)

    model_archive_prefix = "autoviml_exp"
    os.makedirs(model_archive_prefix, exist_ok=True)

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


def run():
    feature_matrix = get_all_feature_matrix()
    # feature_matrix = feature_matrix*1
    # feature_matrix[feature_matrix.columns] = feature_matrix[feature_matrix.columns]\
    #     .apply(pd.to_numeric, errors='coerce', axis=1).fillna(0)

    # pycaret_enable_colab()
    # train_pycaret_w_featurematrix(feature_matrix)
    use_auto_viml(feature_matrix)


if __name__ == "__main__":
    run()
