
import pycaret.classification as pc
from pycaret.utils import enable_colab as pycaret_enable_colab


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


def train_pycaret_w_orimatrix(order_df):
    exp = pc.setup(
        data=order_df,
        target="label",
        session_id=123,
    )

    best_model = pc.compare_models(sort="AUC")

    print(pc.pull())
    print(best_model)



def run():
    train_pycaret_w_orimatrix()


if __name__ == "__main__":
    run()
