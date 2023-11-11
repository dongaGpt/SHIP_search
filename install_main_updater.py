import os
import sys
import time
working_dir = os.getcwd()

service_config = f"""[Unit]
Description= main_updater

[Service]
User=root
WorkingDirectory={working_dir}
Environment="PATH={sys.prefix}/bin"
ExecStart={sys.executable} -u {working_dir}/main_updater.py
Restart=always

[Install] 
WantedBy=multi-user.target 
""".replace('\\', '/')

service_file = f"/etc/systemd/system/da_main_updater.service"
with open(service_file, "w") as f:
    f.write(service_config)
time.sleep(1)
os.system("sudo systemctl daemon-reload")
os.system(f"sudo systemctl start da_main_updater.service")  
os.system(f"sudo systemctl enable da_main_updater.service")
##################################################
print("완료")