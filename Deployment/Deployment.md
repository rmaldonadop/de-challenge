# Deployment

# Database 

## Requeriments
- SQL Server 
- SQL Management Studio

## Database deployment
- Use SQL Management Studio to connect to the SQL Server host
- In the object explorer tab, rigth click on databases directory and choose "Deploy Data-tier Application..." option
- In the select package section, select the "etl_DB.dacpac" file in the folder Deployment\database
- Finally, in the next step, name you database like "ETL_DB" aand finish the process.

# Python Code

## Requeriments
- Python 3.7

## Python Code deployment
- create a directory for the deployment
- copy the requirements.txt, config.ini and etl-db.py files in the depoy directory (from the python folder)
```
mkdir "deploy"
cd deploy
```
- create the virtual enviroment
```
python -m venv python-etl
```
- activate the virtual enviroment
```
windows: tutorial-env\Scripts\activate.bat
Mac or Unix: source tutorial-env/bin/activate
```
- install the requirements with pip using the requirements.txt file
```
pip install -r requirements.txt
```
you are ready to run the python code
```
python etl-db.py
```

## note
- edit the config.ini file to set the required parameters