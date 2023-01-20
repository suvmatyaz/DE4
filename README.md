# DE4

Требования:
  1. Установите пакет Kaggle Public API - pip install kaggle (https://github.com/Kaggle/kaggle-api)
  2. После регистрации на https://www.kaggle.com/ создайте API Token (Account - Create New API Token)
  3. Поместите скачанный kaggle.json в ~/.kaggle/kaggle.json (для Windows в C:\Users\<Windows-username>\.kaggle\kaggle.json)
  4. Запуск через в spark-submit: master = yarn, deploy-mode = client
  
Запуск:
  spark-submit spark-submit --master yarn --deploy-mode client de4_pyspark.py 'AnalyzeBoston/crimes-in-boston' '/home/user/de4' 'path/to/output_folder'
  
  Аргументы:
  1. Наименование датасета в Kaggle
  2. Локальная директория для скачивания датасета
  3. Директория в hdfs для записи результирующего файла
