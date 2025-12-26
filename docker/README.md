\# Docker Deployment Guide for E-Commerce Data Pipeline



\## 1. Prerequisites



Before running the pipeline in Docker, ensure your system meets the following requirements:



\* \*\*Docker:\*\* >= 24.0

\* \*\*Docker Compose:\*\* >= 2.0

\* \*\*Memory:\*\* Minimum 4GB free

\* \*\*Disk Space:\*\* Minimum 5GB free

\* \*\*Python:\*\* Already included in Docker image



\## 2. Quick Start Guide



\### Step 1: Build Docker Images



```bash

docker-compose -f docker/docker-compose.yml build

```



This will build the necessary images for:



\* \*\*PostgreSQL database\*\*

\* \*\*ETL pipeline\*\*



\### Step 2: Start Services



```bash

docker-compose -f docker/docker-compose.yml up -d

```



\* `-d` runs containers in detached mode

\* This will start \*\*PostgreSQL\*\* first, then the \*\*pipeline container\*\*





\### Step 3: Verify Services



Check running containers:



```bash

docker ps

```



You should see containers for:



\* `postgres`

\* `etl\_pipeline` (or your pipeline container name)



Check logs:



```bash

docker logs -f <container\_name>

```



\* PostgreSQL: Ensure no errors

\* Pipeline: Look for successful execution messages





\### Step 4: Run Pipeline (Optional)



If your pipeline container is set up for manual execution:



```bash

docker exec -it <pipeline\_container\_name> python scripts/pipeline\_orchestrator.py

```



\### Step 5: Access Database



```bash

docker exec -it <postgres\_container\_name> psql -U postgres -d ecommerce\_db

```



\### Step 6: Stop Services



```bash

docker-compose -f docker/docker-compose.yml down

```



\* Stops all containers

\* Keeps volumes unless `-v` is added





\### Step 7: Clean Up (Optional)



Remove volumes, networks, and images:



```bash

docker-compose -f docker/docker-compose.yml down -v --rmi all

```





\## 3. Troubleshooting



| Issue                      | Solution                                                        |

| -------------------------- | --------------------------------------------------------------- |

| Port 5432 already in use   | Stop conflicting service or change port in `docker-compose.yml` |

| Database not ready         | Wait a few seconds, PostgreSQL container may still initialize   |

| Volume permission denied   | Ensure Docker has access to project folder                      |

| Container fails to start   | Check logs using `docker logs <container\_name>`                 |

| Network connectivity issue | Ensure services are on the same Docker network (default)        |





\## 4. Configuration



\* \*\*Environment variables\*\* are defined in `docker-compose.yml`



```yaml

POSTGRES\_USER: test\_user

POSTGRES\_PASSWORD: test\_password

POSTGRES\_DB: ecommerce\_db

```



\* \*\*Volumes\*\*:



```yaml

volumes:

&nbsp; postgres\_data:/var/lib/postgresql/data

```



\* \*\*Network:\*\* Default Docker network (containers can communicate using service names)

\* \*\*Resource Limits\*\* (optional, can be added to `docker-compose.yml`):



```yaml

deploy:

&nbsp; resources:

&nbsp;   limits:

&nbsp;     cpus: "1.0"

&nbsp;     memory: 1g

```

