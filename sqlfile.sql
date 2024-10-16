-- Query to retrieve employee details from the employees table
SELECT
    employee_id,
    first_name,
    last_name,
    email,
    phone_number,
    hire_date,
    job_id,
    salary,
    commission_pct,
    manager_id,
    department_id,
    address_id,
    country_id,
    region_id,
    date_of_birth,
    gender,
    status
FROM
    employees
WHERE
    department_id = 90
    AND salary > 60000
    AND hire_date >= '2020-01-01'
    AND job_id IN ('IT_PROG', 'HR_REP', 'SA_REP')
    AND (status = 'active' OR status = 'probation')
    AND (gender = 'M' OR gender = 'F')
ORDER BY
    last_name ASC,
    first_name ASC;

-- Additional query to join with departments table
SELECT
    e.employee_id,
    e.first_name,
    e.last_name,
    e.salary,
    d.department_name
FROM
    employees e
JOIN
    departments d ON e.department_id = d.department_id
WHERE
    d.location_id = 1000
    AND e.salary > 50000;

-- Query to find employees with specific job roles
SELECT
    employee_id,
    first_name,
    last_name,
    job_id
FROM
    employees
WHERE
    job_id IN ('IT_PROG', 'HR_REP', 'SA_REP')
    AND hire_date BETWEEN '2019-01-01' AND '2022-12-31';
-- Query to find employees with specific job roles
SELECT
    employee_id,
    first_name,
    last_name,
    job_id
FROM
    employees
WHERE
    job_id IN ('IT_PROG', 'HR_REP', 'SA_REP')
    AND hire_date BETWEEN '2019-01-01' AND '2022-12-31';

-- Count of employees in each department
SELECT
    department_id,
    COUNT(*) AS employee_count
FROM
    employees
GROUP BY
    department_id
HAVING
    COUNT(*) > 5;

-- Average salary of employees by job
SELECT
    job_id,
    AVG(salary) AS average_salary
FROM
    employees
GROUP BY
    job_id
ORDER BY
    average_salary DESC;

-- List employees hired in the last 5 years
SELECT
    employee_id,
    first_name,
    last_name,
    hire_date
FROM
    employees
WHERE
    hire_date >= DATEADD(YEAR, -5, GETDATE())
ORDER BY
    hire_date DESC;

-- Details of employees with commission
SELECT
    employee_id,
    first_name,
    last_name,
    commission_pct
FROM
    employees
WHERE
    commission_pct IS NOT NULL
ORDER BY
    commission_pct DESC;
-- List employees with their addresses
SELECT
    e.employee_id,
    e.first_name,
    e.last_name,
    a.street_address,
    a.city,
    a.state,
    a.zip_code
FROM
    employees e
JOIN
    addresses a ON e.address_id = a.address_id
WHERE
    a.city = 'New York';

-- Retrieve employees and their managers
SELECT
    e.employee_id AS employee_id,
    e.first_name AS employee_first_name,
    e.last_name AS employee_last_name,
    m.first_name AS manager_first_name,
    m.last_name AS manager_last_name
FROM
    employees e
LEFT JOIN
    employees m ON e.manager_id = m.employee_id
WHERE
    e.manager_id IS NOT NULL;

-- List of employees by region
SELECT
    r.region_id,
    e.employee_id,
    e.first_name,
    e.last_name
FROM
    employees e
JOIN
    regions r ON e.region_id = r.region_id
ORDER BY
    r.region_id, e.last_name;

-- Final overview of employees
SELECT
    e.employee_id,
    e.first_name,
    e.last_name,
    e.salary,
    d.department_name,
    r.region_name
FROM
    employees e
JOIN
    departments d ON e.department_id = d.department_id
JOIN
    regions r ON d.region_id = r.region_id
WHERE
    e.hire_date >= '2015-01-01'
ORDER BY
    e.salary DESC;



