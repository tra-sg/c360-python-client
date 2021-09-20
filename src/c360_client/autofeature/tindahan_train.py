import pandas as pd
import xgboost as xgb
from datetime import datetime
from c360_client.autofeature import get_output_path, convert_types
from matplotlib import rcParams
import matplotlib.pyplot as plt

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


def train_xgboost_w_featurematrix(fm):
    hyparams = {
        "max_depth": 8,
        "min_child_weight": 0.9,
    }
    # model = xgb.XGBClassifier(seed=100, **hyparams)
    model = xgb.XGBRegressor(seed=100, **hyparams)

    columns_todrop = [
        "outlet", "time",
        *[i for i in fm.columns if "order_item.so_document)" in i],
        *[i for i in fm.columns if "order_item.sku)" in i],
        *[i for i in fm.columns if "order_item.distributor)" in i],
        *[i for i in fm.columns if "order_item.online_portion)" in i],

    ]

    model.fit(fm.drop(["label", *columns_todrop], axis=1), fm["label"])

    plt.tight_layout()

    xgb.plot_importance(model, max_num_features=40)

    plt.savefig(
        f"autofeature_model_importance_{datetime.now()}.png"
    )

    model.save_model(f"autofeature_model_importance_{datetime.now()}.save.model")
    # model.dump_model(f"autofeature_model_importance_{datetime.now()}.dump.model")

    return model


if __name__ == "__main__":
    feature_matrix = get_all_feature_matrix()
    feature_matrix = feature_matrix*1
    feature_matrix[feature_matrix.columns] = feature_matrix[feature_matrix.columns]\
        .apply(pd.to_numeric, errors='coerce', axis=1).fillna(0)
    train_xgboost_w_featurematrix(feature_matrix)