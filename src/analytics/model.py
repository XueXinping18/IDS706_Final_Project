import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, r2_score
import joblib
from pathlib import Path

def train_model():
    # 1. Load Data
    data_path = Path("analytics_data.csv")
    if not data_path.exists():
        print("No data found. Run etl.py first.")
        return

    df = pd.read_csv(data_path)
    print(f"Loaded {len(df)} records.")
    
    # 2. Prepare Features (X) and Target (Y)
    # Target: Difficulty Score (Flesch-Kincaid)
    # Features: WPM, Word Count (as a proxy for length/density)
    # Note: In a real scenario, we'd want more distinct features like "Audio Pitch Variance" etc.
    
    X = df[["wpm", "word_count"]]
    y = df["difficulty_score"]
    
    # 3. Split Data
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # 4. Train Model
    model = LinearRegression()
    model.fit(X_train, y_train)
    
    # 5. Evaluate
    y_pred = model.predict(X_test)
    mse = mean_squared_error(y_test, y_pred)
    r2 = r2_score(y_test, y_pred)
    
    print("Model Performance:")
    print(f"MSE: {mse:.4f}")
    print(f"R2 Score: {r2:.4f}")
    print("Coefficients:", model.coef_)
    
    # 6. Save Model
    model_path = Path("src/analytics/difficulty_model.joblib")
    joblib.dump(model, model_path)
    print(f"Saved model to {model_path}")

    # 7. Generate Graph (WPM vs Difficulty)
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
        
        plt.figure(figsize=(10, 6))
        
        # Scatter plot of actual data
        sns.scatterplot(data=df, x='wpm', y='difficulty_score', alpha=0.6, label='Actual Data')
        
        # Regression Line (using the model we just trained)
        # We need to predict y for a range of x values, holding other features constant (or just plotting the trend)
        # For simplicity in 2D, let's plot the trend line for WPM ignoring word_count for a moment, 
        # OR better, just use seaborn's regplot which fits a univariate regression for visualization
        sns.regplot(data=df, x='wpm', y='difficulty_score', scatter=False, color='red', label='Trend Line')
        
        plt.title('Difficulty Score vs. Words Per Minute (WPM)')
        plt.xlabel('Words Per Minute (WPM)')
        plt.ylabel('Difficulty Score (Flesch-Kincaid)')
        plt.legend()
        plt.grid(True, alpha=0.3)
        
        # Save plot
        plot_path = Path("screenshots/regression_plot.png")
        plot_path.parent.mkdir(exist_ok=True)
        plt.savefig(plot_path)
        print(f"Saved regression plot to {plot_path}")
        
    except ImportError:
        print("Matplotlib/Seaborn not installed, skipping plot generation.")
    except Exception as e:
        print(f"Error generating plot: {e}")

if __name__ == "__main__":
    train_model()
