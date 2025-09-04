import os, joblib, numpy as np
from sklearn.linear_model import LogisticRegression
MODEL_PATH=os.environ.get("SUPERVISED_PATH","models/model_supervised.pkl")
def train_or_load(X,y):
    if os.path.exists(MODEL_PATH): return joblib.load(MODEL_PATH)
    m=LogisticRegression(max_iter=1000); m.fit(X,y)
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True); joblib.dump(m, MODEL_PATH)
    return m
def predict_proba(m,X): return m.predict_proba(X)[:,1]
