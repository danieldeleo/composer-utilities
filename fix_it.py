import re

with open("/workspace/cicd/dags/sleepy_task_group.py") as f:
    content = f.read()

# Fix the seconds variable access bug. Airflow handles XComArg resolution automatically.
# Calling list() on a lazy sequence of XComArgs from dynamic task mapping can sometimes fail 
# depending on the context or Airflow version. We can just iterate over it or print it.
content = content.replace("        seconds = list(seconds)", "        pass")

with open("/workspace/cicd/dags/sleepy_task_group.py", "w") as f:
    f.write(content)
