import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from imblearn.over_sampling import SMOTE
from sklearn.ensemble import GradientBoostingClassifier

def model(features_model: pd.DataFrame):

    df = features_model

    # Eliminar no numericos y redundantes (time_since_last_res es la regla para determinar el valor de la variable objetivo)
    columns_to_drop = ['client_key', 'last_visit', 'last_reservation', 'time_since_last_res']
    df.drop(columns_to_drop, axis = 1, inplace = True)
    # Eliminar columnas con correlación alta
    df.drop('total_people_stayed', axis = 1, inplace = True)

    X = df.drop('churn', axis=1)
    y = df['churn']

    # Escalar los datos
    scaler = StandardScaler()
    X = scaler.fit_transform(X)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Resamplear el X y Y train para que estén balanceados
    sm = SMOTE(random_state=42)
    X_res, y_res = sm.fit_resample(X_train, y_train)

    # Inicializar modelo con parametros óptimos y pesos para balancear clases

    model = GradientBoostingClassifier(n_estimators=300,min_samples_split=2,min_samples_leaf=1,
                                    max_features='log2',max_depth=None,random_state = 42)

    # Predecir en el test set
    model.fit(X_res, y_res)
    y_pred = model.predict(X_test)

    # Guardar modelo
    model_data = {
        'model': model,
        'X_train': X_train,
        'X_test': X_test,
        'y_train': y_train,
        'y_test': y_test,
        'y_pred': y_pred
    }

    return model_data



