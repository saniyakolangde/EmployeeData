# Databricks notebook source
# MAGIC %md 
# MAGIC ### Employee Data Processing
# MAGIC 
# MAGIC We have 2 datasets; General employee details, and survey results where employees have ranked their environmental satisfaction, job involment level, and work life balance.
# MAGIC 
# MAGIC Satisfaction and Involvement leves ranked from 1 - 4 where 1 being low, 2 Medium, 3 High, and 4 Very High.
# MAGIC 
# MAGIC ###### 1) We will first get details of employees
# MAGIC ###### 2) We will use joins to merge relevant information
# MAGIC ###### 3) We will find the highest and lowest paid employees
# MAGIC ###### 4) We will check if employees are more satisfied/dissatisfied
# MAGIC ###### 5) We will find co-relation between survey results and employee performance

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Load csv files

# COMMAND ----------

import pandas as pd
#Read dataset and create df
employee_data = spark.read.csv("/mnt/raw/data/source=retail/Saniya/dataset=employee-data/employee_data.csv", header = "true")
employee_data = employee_data.toPandas()
employee_data.describe()

# COMMAND ----------

employee_survey_data = spark.read.csv("/mnt/raw/data/source=retail/Saniya/dataset=employee-data/employee_survey_data.csv", header = "true")
employee_survey_data = employee_survey_data.toPandas()
employee_survey_data.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Run Queries on data loaded

# COMMAND ----------

#get employee ID, salary, working years, salary hike and survey results
joinedDF = pd.merge(employee_data, employee_survey_data, on=["EmployeeID"])
joinedDF[['EmployeeID', 'MonthlyIncome', 'TotalWorkingYears', 'PercentSalaryHike', 'JobSatisfaction', 'WorkLifeBalance', 'EnvironmentSatisfaction']]


# COMMAND ----------

#get employee ID, salary, working years, salary hike for workers whose job satisfaction is LOW
joinedDF = pd.merge(employee_data, employee_survey_data, on=["EmployeeID"], how='left')
display(joinedDF)
joinedDF[joinedDF['JobSatisfaction'] == '1'][['EmployeeID', 'MonthlyIncome', 'TotalWorkingYears', 'PercentSalaryHike', 'JobSatisfaction']]

# COMMAND ----------

#find highest paid employees survey results
joinedDF = pd.merge(employee_data, employee_survey_data, on=["EmployeeID"])
maxIncome = joinedDF['MonthlyIncome'].max()
joinedDF[joinedDF['MonthlyIncome'] == maxIncome][['EmployeeID', 'MonthlyIncome', 'JobSatisfaction', 'WorkLifeBalance', 'EnvironmentSatisfaction']]

# COMMAND ----------

#find lowest paid employees survey results
joinedDF = pd.merge(employee_data, employee_survey_data, on=["EmployeeID"])
minIncome = joinedDF['MonthlyIncome'].min()
joinedDF[joinedDF['MonthlyIncome'] == minIncome][['EmployeeID', 'MonthlyIncome', 'JobSatisfaction', 'WorkLifeBalance', 'EnvironmentSatisfaction']]

# COMMAND ----------

#Count Survey levels
a = employee_survey_data.groupby('JobSatisfaction').size()
b = employee_survey_data.groupby('WorkLifeBalance').size()
c = employee_survey_data.groupby('EnvironmentSatisfaction').size()
result = pd.concat([a, b, c], axis = 1)
result.columns = ['JobSatisfaction', 'WorkLifeBalance', 'EnvironmentSatisfaction']
result.index = ['1', '2', '3', '4', 'NA']
print(result)

# COMMAND ----------

#Find employee details of Highest Survey results 
joinedDF = pd.merge(employee_data, employee_survey_data, on=["EmployeeID"])
FinalDF = joinedDF[(joinedDF['JobSatisfaction'] == '4') & (joinedDF['WorkLifeBalance'] == '4') & (joinedDF['EnvironmentSatisfaction'] == '4')][['EmployeeID', 'MonthlyIncome', 'TotalWorkingYears', 'PercentSalaryHike', 'JobSatisfaction', 'WorkLifeBalance', 'EnvironmentSatisfaction']]
display(FinalDF)

# COMMAND ----------

#Find employee details of Lowest Survey results 
joinedDF = pd.merge(employee_data, employee_survey_data, on=["EmployeeID"])
FinalDF = joinedDF[(joinedDF['JobSatisfaction'] == '1') & (joinedDF['WorkLifeBalance'] == '1') & (joinedDF['EnvironmentSatisfaction'] == '1')][['EmployeeID', 'MonthlyIncome', 'TotalWorkingYears', 'PercentSalaryHike', 'JobSatisfaction', 'WorkLifeBalance', 'EnvironmentSatisfaction']]
display(FinalDF)

# COMMAND ----------

#pivot to see count of survey result for Job Satisfaction
pd.pivot_table(employee_survey_data, values = 'EmployeeID', columns = 'JobSatisfaction', aggfunc='count')

# COMMAND ----------

#Display MAX and MIN Income in departments
pd.pivot_table(employee_data, values = 'MonthlyIncome', columns = 'Department', aggfunc=['max', 'min'])
