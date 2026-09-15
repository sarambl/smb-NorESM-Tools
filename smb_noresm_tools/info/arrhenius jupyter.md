# How to get environment on arrhenius:

HOME: /home/<user_name>
PROJECT REPO: /nobackup/proj/disk/luftlab/personal/<user_name>

**In home:**

```
module spider python
module load Python/3.13.5-bundle-SciPy-2025.07-mpi4py-4.1.0-gcc-2025b-eb
python -m venv <path_and_name_of_env>
source <path_and_name_of_env>/bin/activate
```


Install jupyterlab:

```bash
 pip install jupyterlab
 pip install -U jupyter_server
```



TO ACCESS YOUR JUPYTER LAB:

```bash
ssh -L 8889:localhost:8889 <user_name>@login.hpc.arrhenius.naiss.se
cd <PROJECT_REPO>
jupyter-lab --port 8889
```

On your computer in browser, open http://localhost:8889/lab/workspaces/auto-D. 
