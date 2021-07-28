-- Databricks notebook source
-- MAGIC %md 
-- MAGIC ### Employee Data Processing
-- MAGIC 
-- MAGIC We have 2 datasets; General employee details, and survey results where employees have ranked their environmental satisfaction, job involment level, and work life balance.
-- MAGIC 
-- MAGIC Satisfaction and Involvement leves ranked from 1 - 4 where 1 being low, 2 Medium, 3 High, and 4 Very High.
-- MAGIC 
-- MAGIC ###### 1) We will first get details of employees
-- MAGIC ###### 2) We will use joins to merge relevant information
-- MAGIC ###### 3) We will find the highest and lowest paid employees
-- MAGIC ###### 4) We will check if employees are more satisfied/dissatisfied
-- MAGIC ###### 5) We will find co-relation between survey results and employee performance

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Load csv files

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW employee_data 
USING CSV
OPTIONS (PATH "/mnt/raw/data/source=retail/Saniya/dataset=employee-data/employee_data.csv", header "true", mode "FAILFAST");

CREATE OR REPLACE TEMPORARY VIEW employee_survey_data 
USING CSV
OPTIONS (PATH "/mnt/raw/data/source=retail/Saniya/dataset=employee-data/employee_survey_data.csv", header "true", mode "FAILFAST");

-- COMMAND ----------

DESCRIBE employee_data;

-- COMMAND ----------

DESCRIBE employee_survey_data;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Run Queries on data loaded

-- COMMAND ----------

--get employee ID, salary, working years, salary hike and survey results
SELECT employee_data.EmployeeID, employee_data.MonthlyIncome, employee_data.TotalWorkingYears, employee_data.PercentSalaryHike, employee_survey_data.JobSatisfaction, employee_survey_data.WorkLifeBalance, employee_survey_data.EnvironmentSatisfaction
FROM employee_data
INNER JOIN employee_survey_data
ON employee_data.EmployeeID = employee_survey_data.EmployeeID
ORDER BY employee_data.PercentSalaryHike; 

-- COMMAND ----------

--get employee ID, salary, working years, salary hike for workers whose job satisfaction is LOW
SELECT employee_data.EmployeeID, employee_data.MonthlyIncome, employee_data.TotalWorkingYears, employee_data.PercentSalaryHike, employee_survey_data.JobSatisfaction
FROM employee_data
LEFT JOIN employee_survey_data
ON employee_data.EmployeeID = employee_survey_data.EmployeeID AND employee_survey_data.JobSatisfaction = 1
ORDER BY employee_data.PercentSalaryHike;

-- COMMAND ----------

--find highest paid employees survey results
SELECT employee_data.EmployeeID, employee_data.MonthlyIncome, employee_survey_data.JobSatisfaction, employee_survey_data.WorkLifeBalance, employee_survey_data.EnvironmentSatisfaction
FROM employee_data
INNER JOIN employee_survey_data
ON employee_data.EmployeeID = employee_survey_data.EmployeeID
WHERE MonthlyIncome = (SELECT MAX(MonthlyIncome) 
                       FROM employee_data
                       WHERE EmployeeID = employee_data.EmployeeID);

-- COMMAND ----------

--find lowest paid empoyees survey results
SELECT employee_data.EmployeeID, employee_data.MonthlyIncome, employee_survey_data.JobSatisfaction, employee_survey_data.WorkLifeBalance, employee_survey_data.EnvironmentSatisfaction
FROM employee_data
INNER JOIN employee_survey_data
ON employee_data.EmployeeID = employee_survey_data.EmployeeID
WHERE MonthlyIncome = (SELECT MIN(MonthlyIncome) 
                       FROM employee_data
                       WHERE EmployeeID = employee_data.EmployeeID);

-- COMMAND ----------

--Change deparment names
SELECT UPPER(REPLACE(Department, 'Research & Development', 'R&D')) as Department
FROM employee_data;

-- COMMAND ----------

--Count Survey levels
SELECT t1.Level, JobSatisfactionCount, WorkLifeBalanceCount, EnvironmentSatisfactionCount 
FROM(
    SELECT employee_survey_data.JobSatisfaction as Level, COUNT(employee_survey_data.JobSatisfaction) as JobSatisfactionCount
    FROM employee_survey_data
    GROUP BY employee_survey_data.JobSatisfaction) t1
      JOIN(
          SELECT employee_survey_data.WorkLifeBalance as Level, COUNT(employee_survey_data.WorkLifeBalance) as WorkLifeBalanceCount
          FROM employee_survey_data
          GROUP BY employee_survey_data.WorkLifeBalance) t2
           ON (t1.Level = t2.Level)
              JOIN(
                  SELECT employee_survey_data.EnvironmentSatisfaction as Level, COUNT(employee_survey_data.EnvironmentSatisfaction) as EnvironmentSatisfactionCount
                  FROM employee_survey_data
                  GROUP BY employee_survey_data.EnvironmentSatisfaction) t3
                  ON (t1.Level = t3.Level)
ORDER BY Level

-- COMMAND ----------

--Find employee details of Highest Survey results 
SELECT employee_data.EmployeeID, employee_data.PercentSalaryHike, employee_data.MonthlyIncome, employee_data.TotalWorkingYears, employee_survey_data.JobSatisfaction, employee_survey_data.WorkLifeBalance, employee_survey_data.EnvironmentSatisfaction
FROM employee_data
JOIN employee_survey_data
ON employee_data.EmployeeID = employee_survey_data.EmployeeID
WHERE employee_survey_data.JobSatisfaction = '4' AND employee_survey_data.WorkLifeBalance = '4' AND employee_survey_data.EnvironmentSatisfaction = '4'
ORDER BY employee_data.MonthlyIncome;

-- COMMAND ----------

--Find employee details of Lowest Survey results
SELECT employee_data.EmployeeID, employee_data.PercentSalaryHike, employee_data.MonthlyIncome, employee_data.TotalWorkingYears, employee_survey_data.JobSatisfaction, employee_survey_data.WorkLifeBalance, employee_survey_data.EnvironmentSatisfaction
FROM employee_data
JOIN employee_survey_data
ON employee_data.EmployeeID = employee_survey_data.EmployeeID
WHERE employee_survey_data.JobSatisfaction = '1' AND employee_survey_data.WorkLifeBalance = '1' AND employee_survey_data.EnvironmentSatisfaction = '1'
ORDER BY employee_data.MonthlyIncome;

-- COMMAND ----------

--pivot to see count of survey result for Job Satisfaction
SELECT * FROM (
  SELECT JobSatisfaction FROM employee_survey_data)
  PIVOT (
    COUNT(JobSatisfaction)
    FOR JobSatisfaction IN (1 LOW, 2 MEDIUM, 3 HIGH, 4 VERYHIGH, 'NA')
  )

-- COMMAND ----------

--Display MAX and MIN Income in departments
Select * FROM (
SELECT Department, MonthlyIncome FROM employee_data)
PIVOT (
  MAX(MonthlyIncome) as max, MIN(MonthlyIncome) as min
  FOR Department IN ('Human Resources', 'Sales', 'Research & Development')
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Drawing conclusions
-- MAGIC ###### 1) As Salary hike increases, satistfaction level increases
-- MAGIC ###### 2) as working years increase, satisfaction level decrease
-- MAGIC ###### 3) Employees are moderatly satisfied with Work Life Balance
-- MAGIC ###### 4) Employees are very satisfied with Environment
-- MAGIC ###### 5) Employees are satisfied with Job Satisfaction
