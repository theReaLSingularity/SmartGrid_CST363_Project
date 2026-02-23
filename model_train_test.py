"""
train_keras_daily.py

Trains a simple Keras model for next-day energy demand forecasting using a
"predict-the-residual" strategy:
    y_delta = y_next - consumption_today

This usually outperforms or at least matches a strong naive baseline:
    y_next_hat = consumption_today  (i.e., delta_hat = 0)

Requirements:
  pip install psycopg2-binary pandas numpy scikit-learn tensorflow joblib

Run:
  python train_keras_daily.py

Outputs:
  artifacts/model.keras
  artifacts/scaler.joblib
"""

from __future__ import annotations

import os
import joblib

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import psycopg2

from sklearn.metrics import mean_absolute_error
from sklearn.metrics import r2_score
from sklearn.preprocessing import StandardScaler

from tensorflow import keras
from tensorflow.keras import layers
from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau


# -------------------------
# Config
# -------------------------

DB = {
    "host": "localhost",
    "port": 5435,
    "dbname": "smartgrid_db",
    "user": "smart",
    "password": "smart1",
}

QUERY = """
SELECT *
FROM features_daily
ORDER BY date;
"""

ARTIFACT_DIR = "artifacts"
MODEL_PATH = os.path.join(ARTIFACT_DIR, "model.keras")
SCALER_PATH = os.path.join(ARTIFACT_DIR, "scaler.joblib")

# Use enough history so rolling windows stabilize (you used up to 30-day)
MIN_HISTORY_CUTOFF = 35

EPOCHS = 30
BATCH_SIZE = 32
RANDOM_SEED = 42


# -------------------------
# Helpers
# -------------------------

def plot_predictions(test_df, y_true, y_pred, last_n=200):
    """
    Plots actual vs predicted values for the test period.

    Parameters:
        test_df : pandas DataFrame (must contain 'date')
        y_true  : numpy array of actual y_next values
        y_pred  : numpy array of predicted y_next values
        last_n  : number of final points to display (default 200)
    """

    dates = test_df["date"].iloc[-last_n:]
    actual = y_true[-last_n:]
    predicted = y_pred[-last_n:]

    mae = np.mean(np.abs(actual - predicted))

    plt.figure(figsize=(12, 6))
    plt.plot(dates, actual, label="Actual", linewidth=2)
    plt.plot(dates, predicted, label="Predicted", linestyle="--")
    plt.title(f"Next-Day Energy Demand Prediction\nMAE = {mae:.4f}")
    plt.xlabel("Date")
    plt.ylabel("Consumption (kWh)")
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig("plots/Prediction_vs_Actual.png")
    plt.show()


def ensure_artifacts_dir() -> None:
    os.makedirs(ARTIFACT_DIR, exist_ok=True)


def load_from_postgres() -> pd.DataFrame:
    with psycopg2.connect(**DB) as conn:
        df = pd.read_sql(QUERY, conn)
    return df


def coerce_types(df: pd.DataFrame) -> pd.DataFrame:
    # Date handling
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)

    # Convert Decimal-ish / object numeric columns to float
    # (NUMERIC from Postgres can arrive as Decimal objects)
    for c in df.columns:
        if c != "date" and df[c].dtype == "object":
            df[c] = df[c].astype(float)

    return df


def time_split(df: pd.DataFrame, train_frac: float = 0.9) -> tuple[pd.DataFrame, pd.DataFrame]:
    split_idx = int(len(df) * train_frac)
    return df.iloc[:split_idx].copy(), df.iloc[split_idx:].copy()


def build_xy(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Returns:
      X: features (float32)
      y_next: absolute next-day consumption (float32) -- for evaluation
      y_delta: residual target (float32) = y_next - consumption_today
    """
    # residual target
    df["y_delta"] = df["y_next"].astype("float32") - df["consumption"].astype("float32")

    y_next = df["y_next"].astype("float32").to_numpy()
    y_delta = df["y_delta"].astype("float32").to_numpy()

    # Model inputs: everything except date and labels
    drop_cols = {"date", "y_next", "y_delta"}
    X = df.drop(columns=[c for c in drop_cols if c in df.columns]).astype("float32").to_numpy()

    return X, y_next, y_delta


# -------------------------
# Main
# -------------------------

def main() -> None:
    np.random.seed(RANDOM_SEED)
    keras.utils.set_random_seed(RANDOM_SEED)

    ensure_artifacts_dir()

    df = load_from_postgres()
    df = coerce_types(df)

    # Drop rows with NaNs (rolling stats / lag features create NaNs early on)
    df = df.dropna().reset_index(drop=True)

    # OPTIONAL: Drop the first N days to ensure all windows are stable
    # (Useful if your SQL used 30-day rolling windows)
    if len(df) > MIN_HISTORY_CUTOFF:
        df = df.iloc[MIN_HISTORY_CUTOFF:].reset_index(drop=True)

    train_df, test_df = time_split(df, train_frac=0.9)

    # Build X and targets
    X_train, y_next_train, y_delta_train = build_xy(train_df)
    X_test, y_next_test, y_delta_test = build_xy(test_df)

    # Baselines
    # Persistence baseline: y_next_hat = consumption_today
    baseline_pred = test_df["consumption"].astype("float32").to_numpy()
    baseline_mae = float(np.mean(np.abs(baseline_pred - y_next_test)))
    print("Baseline MAE (predict y_next = consumption):", baseline_mae)

    # Scale features
    scaler = StandardScaler()
    X_train = scaler.fit_transform(X_train)
    X_test = scaler.transform(X_test)

    # Simple model predicts y_delta
    model = keras.Sequential([
        layers.Input(shape=(X_train.shape[1],)),
        layers.Dense(64, activation="relu"),
        layers.Dense(32, activation="relu"),
        layers.Dense(1),  # y_delta
    ])

    model.compile(optimizer="adam", loss="mse", metrics=["mae"])

    # For time-series: do NOT shuffle
    callbacks = [
        EarlyStopping(monitor="val_mae", patience=15, restore_best_weights=True),
        ReduceLROnPlateau(monitor="val_mae", factor=0.5, patience=7, min_lr=1e-5),
    ]

    model.fit(
        X_train,
        y_delta_train,
        validation_split=0.2,
        epochs=200,
        batch_size=32,
        shuffle=False,
        callbacks=callbacks,
        verbose=1,
    )

    # Predict residuals and reconstruct y_next
    delta_pred = model.predict(X_test, verbose=0).reshape(-1)
    y_pred = test_df["consumption"].astype("float32").to_numpy() + delta_pred



    plot_predictions(test_df, y_next_test, y_pred, last_n=200)

    model_mae = float(np.mean(np.abs(y_pred - y_next_test)))
    print("Model MAE (residual approach):", model_mae)
    print("R2:", r2_score(y_next_test, y_pred))

    def permutation_importance(model, X, y_true, test_df, baseline_mae, feature_names):
        importances = []

        for i, name in enumerate(feature_names):
            X_perm = X.copy()
            np.random.shuffle(X_perm[:, i])

            delta_pred = model.predict(X_perm, verbose=0).reshape(-1)
            y_perm_pred = test_df["consumption"].astype("float32").to_numpy() + delta_pred

            perm_mae = mean_absolute_error(y_true, y_perm_pred)
            importance = perm_mae - baseline_mae
            importances.append((name, importance))

        importances.sort(key=lambda x: x[1], reverse=True)

        return importances

    feature_names = test_df.drop(columns=["date", "y_next", "y_delta"]).columns.tolist()

    importances = permutation_importance(
        model,
        X_test,
        y_next_test,
        test_df,
        model_mae,
        feature_names
    )

    print("\nTop 10 Most Important Features:")
    for name, imp in importances[:10]:
        print(f"{name}: +{imp:.5f} MAE increase")

    # Save artifacts
    model.save(MODEL_PATH)
    joblib.dump(scaler, SCALER_PATH)
    print(f"Saved model -> {MODEL_PATH}")
    print(f"Saved scaler -> {SCALER_PATH}")


if __name__ == "__main__":
    main()