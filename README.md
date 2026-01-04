# Crop-Damage-Simulation-End-to-End-ML-Pipeline
This project demonstrates the full lifecycle of a data science workflow — from custom Python modules that generate randomized datasets with embedded biases to mimic real-world agricultural scenarios, through exploratory data analysis (EDA), feature engineering, and model development, all the way to evaluation and stakeholder storytelling.
# 🌱 Crop Damage Simulation & End-to-End ML Pipeline
**From biased synthetic data to actionable crop insights.**

---

## 🛠️ Project Workflow
1. **Data Generation**
   - Custom classes to simulate crop damage events with controllable biases (region, crop type, seasonality).
   - Randomized datasets designed to mimic real-world agricultural variability.

2. **Exploratory Data Analysis (EDA)**
   - Trend analysis with resampling and rolling windows.
   - Heatmaps comparing categorical vs numerical features.
   - Visual storytelling of seasonal and regional damage patterns.

3. **Feature Engineering**
   - Time-based aggregations (weekly/monthly).
   - Encoding categorical variables (OrdinalEncoder / ColumnTransformer).
   - Scaling numerical features with StandardScaler.

4. **Model Development**
   - Logistic Regression baseline vs Random Forest.
   - Hyperparameter tuning with GridSearchCV.
   - Comparison of training/validation scores against baseline.

5. **Evaluation**
   - Confusion matrix, ROC curves, and classification reports.
   - Feature importance plots to highlight drivers of crop damage.
   - Stakeholder-ready insights balancing accuracy with interpretability.

---

## 📊 Key Results
- Baseline (majority class) vs tuned Random Forest: small but meaningful improvement (+0.02).  
- Best parameters: `max_depth=10`, `min_samples_split=2`, increasing `n_estimators`.  
- Feature importance revealed **region, crop type, and seasonality** as dominant predictors.  
- Visualizations provided actionable insights for resource allocation and risk mitigation.

---

## 🎯 Stakeholder Insights
- **Mean vs Max Damage Trends:** Average weekly damage is stable, but extreme spikes highlight risk weeks.  
- **Regional Vulnerability:** Certain regions consistently show higher damage, guiding targeted interventions.  
- **Model Interpretability:** Logistic regression coefficients and RF feature importances translate technical results into business impact.

---

## 🚀 How to Run
```bash
# Clone the repository
git clone https://github.com/yourusername/crop-damage-pipeline.git
cd crop-damage-pipeline

# Install dependencies
pip install -r requirements.txt

# Run the notebook
jupyter notebook crop_damage.ipynb
