# Hướng Dẫn Dựng HDFS High Availability Cluster Trên Laptop Bằng Docker

> **Mục tiêu:** Dựng HDFS HA cluster theo mô hình QJM (Quorum Journal Manager) với tự động failover, chạy hoàn toàn trên máy tính cá nhân bằng Docker Compose.

---

## Kiến Trúc Tổng Quan

```
                        ┌─────────────┐
                        │  ZooKeeper  │
                        │  Quorum(3)  │
                        │ zk1,zk2,zk3 │
                        └──────┬──────┘
                               │ leader election
              ┌────────────────┼────────────────┐
              │                │                │
       ┌──────▼──────┐         │        ┌───────▼─────┐
       │  namenode1  │ ◄───────┴──────► │  namenode2  │
       │  (Active)   │   FSImage  sync  │  (Standby)  │
       │    ZKFC     │                  │    ZKFC     │
       └──────┬──────┘                  └─────────────┘
              │ write edits logs
     ┌────────┴────────┐
     ▼        ▼        ▼
 ┌───────┐ ┌───────┐ ┌───────┐
 │  jn1  │ │  jn2  │ │  jn3  │   ← JournalNode quorum (3 node)
 └───────┘ └───────┘ └───────┘
              │ block reports
     ┌────────┴────────┐
     ▼                 ▼
┌──────────┐     ┌──────────┐
│datanode1 │     │datanode2 │     ← Lưu trữ dữ liệu thực tế
└──────────┘     └──────────┘
```

### Vai Trò Từng Thành Phần

| Thành phần | Số lượng | Vai trò |
|---|---|---|
| **NameNode** | 2 | Một Active xử lý request, một Standby đồng bộ sẵn sàng thay thế |
| **ZKFC** | 2 | Chạy kèm mỗi NameNode, giám sát sức khỏe và kích hoạt failover tự động |
| **JournalNode** | 3 | Lưu edit log chung, cho phép Standby NN đồng bộ trạng thái với Active NN |
| **ZooKeeper** | 3 | Bầu chọn leader, đảm bảo không bao giờ có 2 Active NameNode cùng lúc |
| **DataNode** | 2 | Lưu trữ block dữ liệu, báo cáo lên NameNode đang Active |

> **Tại sao JournalNode và ZooKeeper đều cần 3?**
> Quorum đòi hỏi đa số (majority) đồng ý mới hợp lệ. Với 3 node, cluster chịu được 1 node chết mà vẫn hoạt động (2/3 vẫn là đa số). Dùng 2 node không có majority — sẽ split-brain.

---

## Yêu Cầu Hệ Thống

| Thành phần | Yêu cầu |
|---|---|
| RAM | 8 GB (khuyến nghị 12 GB) |
| CPU | 4+ nhân |
| Dung lượng trống | 10 GB |
| Docker Desktop | v4.0+ — tăng memory limit lên ≥ 8 GB trong Settings |

---

## Cấu Trúc Thư Mục

```
hdfs-ha-docker/
├── docker-compose.yml
├── Dockerfile.hadoop
└── config/
    ├── core-site.xml
    ├── hdfs-site.xml
    ├── hadoop-env.sh
    └── zoo.cfg
```

```bash
mkdir hdfs-ha-docker && cd hdfs-ha-docker
mkdir config
```

> **Lưu ý:** Không cần `Dockerfile.zookeeper` — ZooKeeper dùng image official và nhận config qua volume mount.

---

## Bước 1: Cấu Hình ZooKeeper

### `config/zoo.cfg`

```properties
tickTime=2000
initLimit=10
syncLimit=5
dataDir=/data

clientPort=2181

# Khai báo 3 node trong quorum
# format: server.ID=hostname:peer_port:leader_port
server.1=zk1:2888:3888
server.2=zk2:2888:3888
server.3=zk3:2888:3888
```

> **Lưu ý quan trọng:** `dataDir=/data` (không phải `/data/zookeeper`) vì đây là đường dẫn mặc định được mount trong image ZooKeeper official.
>
> File này sẽ được mount trực tiếp vào container tại `/conf/zoo.cfg`. Cách này đảm bảo `clientPort=2181` được áp dụng — ZooKeeper 3.8 Docker image không xử lý biến môi trường `ZOO_PORT` để tạo `clientPort` một cách tin cậy.

---

## Bước 2: Cấu Hình HDFS

### `config/core-site.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>

  <!-- Tên logical của cluster HA — client dùng tên này, không hardcode IP -->
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://mycluster</value>
  </property>

  <property>
    <name>hadoop.tmp.dir</name>
    <value>/hadoop/tmp</value>
  </property>

  <!-- Địa chỉ ZooKeeper quorum -->
  <property>
    <name>ha.zookeeper.quorum</name>
    <value>zk1:2181,zk2:2181,zk3:2181</value>
  </property>

</configuration>
```

### `config/hdfs-site.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<configuration>

  <!-- ==================== HA Cluster Setup ==================== -->

  <!-- Tên logical cluster (khớp với fs.defaultFS) -->
  <property>
    <name>dfs.nameservices</name>
    <value>mycluster</value>
  </property>

  <!-- Hai NameNode trong cluster -->
  <property>
    <name>dfs.ha.namenodes.mycluster</name>
    <value>nn1,nn2</value>
  </property>

  <!-- RPC address của từng NameNode -->
  <property>
    <name>dfs.namenode.rpc-address.mycluster.nn1</name>
    <value>namenode1:9000</value>
  </property>
  <property>
    <name>dfs.namenode.rpc-address.mycluster.nn2</name>
    <value>namenode2:9000</value>
  </property>

  <!-- HTTP address (Web UI) -->
  <property>
    <name>dfs.namenode.http-address.mycluster.nn1</name>
    <value>namenode1:9870</value>
  </property>
  <property>
    <name>dfs.namenode.http-address.mycluster.nn2</name>
    <value>namenode2:9870</value>
  </property>

  <!-- ==================== JournalNode ==================== -->

  <!-- Địa chỉ 3 JournalNode — edit log được ghi đồng thời lên cả 3 -->
  <property>
    <name>dfs.namenode.shared.edits.dir</name>
    <value>qjournal://jn1:8485;jn2:8485;jn3:8485/mycluster</value>
  </property>

  <!-- Thư mục lưu dữ liệu JournalNode -->
  <property>
    <name>dfs.journalnode.edits.dir</name>
    <value>/hadoop/dfs/journal</value>
  </property>

  <!-- ==================== Automatic Failover ==================== -->

  <!-- Bật automatic failover qua ZKFC -->
  <property>
    <name>dfs.ha.automatic-failover.enabled</name>
    <value>true</value>
  </property>

  <!-- Fencing: đảm bảo Active cũ bị cô lập trước khi Standby lên Active -->
  <property>
    <name>dfs.ha.fencing.methods</name>
    <value>shell(/bin/true)</value>
  </property>

  <!-- ==================== Proxy Provider ==================== -->

  <!-- Client tự động tìm NameNode đang Active -->
  <property>
    <name>dfs.client.failover.proxy.provider.mycluster</name>
    <value>org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider</value>
  </property>

  <!-- ==================== Storage ==================== -->

  <property>
    <name>dfs.namenode.name.dir</name>
    <value>/hadoop/dfs/name</value>
  </property>
  <property>
    <name>dfs.datanode.data.dir</name>
    <value>/hadoop/dfs/data</value>
  </property>
  <property>
    <name>dfs.replication</name>
    <value>2</value>
  </property>
  <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
  </property>
  <property>
    <name>dfs.webhdfs.enabled</name>
    <value>true</value>
  </property>

</configuration>
```

### `config/hadoop-env.sh`

```bash
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
export HDFS_NAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_JOURNALNODE_USER=root
export HDFS_ZKFC_USER=root
```

> **Lưu ý:** JAVA_HOME được detect động thay vì hardcode đường dẫn cụ thể (`/usr/lib/jvm/java-11-openjdk-amd64`). Cách này hoạt động trên cả máy Intel (amd64) và Apple Silicon (arm64) mà không cần sửa file khi đổi máy.

---

## Bước 3: Tạo `Dockerfile.hadoop`

```dockerfile
FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
    openjdk-11-jdk \
    wget \
    ssh \
    curl \
    netcat \
    && rm -rf /var/lib/apt/lists/*

ENV HADOOP_VERSION=3.4.3
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

RUN wget -q https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    && tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt/ \
    && mv /opt/hadoop-${HADOOP_VERSION} $HADOOP_HOME \
    && rm hadoop-${HADOOP_VERSION}.tar.gz

# SSH không cần password
RUN ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa \
    && cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys \
    && chmod 0600 ~/.ssh/authorized_keys

COPY config/core-site.xml  $HADOOP_HOME/etc/hadoop/core-site.xml
COPY config/hdfs-site.xml  $HADOOP_HOME/etc/hadoop/hdfs-site.xml
COPY config/hadoop-env.sh  $HADOOP_HOME/etc/hadoop/hadoop-env.sh

RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data /hadoop/dfs/journal /hadoop/tmp

EXPOSE 9000 9870 8485 8480
```

---

## Bước 4: Tạo `docker-compose.yml`

```yaml
version: "3.8"

networks:
  hdfs-ha-net:
    driver: bridge

# ─────────────────────────────────────────────
# ZOOKEEPER QUORUM (3 node)
# ─────────────────────────────────────────────
services:

  zk1:
    image: zookeeper:3.8
    container_name: zk1
    hostname: zk1
    networks: [hdfs-ha-net]
    environment:
      ZOO_MY_ID: 1
    volumes:
      - zk1_data:/data
      - ./config/zoo.cfg:/conf/zoo.cfg:ro

  zk2:
    image: zookeeper:3.8
    container_name: zk2
    hostname: zk2
    networks: [hdfs-ha-net]
    environment:
      ZOO_MY_ID: 2
    volumes:
      - zk2_data:/data
      - ./config/zoo.cfg:/conf/zoo.cfg:ro

  zk3:
    image: zookeeper:3.8
    container_name: zk3
    hostname: zk3
    networks: [hdfs-ha-net]
    environment:
      ZOO_MY_ID: 3
    volumes:
      - zk3_data:/data
      - ./config/zoo.cfg:/conf/zoo.cfg:ro

# ─────────────────────────────────────────────
# JOURNALNODE QUORUM (3 node)
# ─────────────────────────────────────────────

  jn1:
    build:
      context: .
      dockerfile: Dockerfile.hadoop
    container_name: jn1
    hostname: jn1
    networks: [hdfs-ha-net]
    volumes:
      - jn1_data:/hadoop/dfs/journal
    command: >
      bash -c "service ssh start && hdfs journalnode"

  jn2:
    build:
      context: .
      dockerfile: Dockerfile.hadoop
    container_name: jn2
    hostname: jn2
    networks: [hdfs-ha-net]
    volumes:
      - jn2_data:/hadoop/dfs/journal
    command: >
      bash -c "service ssh start && hdfs journalnode"

  jn3:
    build:
      context: .
      dockerfile: Dockerfile.hadoop
    container_name: jn3
    hostname: jn3
    networks: [hdfs-ha-net]
    volumes:
      - jn3_data:/hadoop/dfs/journal
    command: >
      bash -c "service ssh start && hdfs journalnode"

# ─────────────────────────────────────────────
# NAMENODE 1 — Active (khởi tạo đầu tiên)
# ─────────────────────────────────────────────

  namenode1:
    build:
      context: .
      dockerfile: Dockerfile.hadoop
    container_name: namenode1
    hostname: namenode1
    networks: [hdfs-ha-net]
    ports:
      - "9870:9870"   # Web UI NN1
      - "9000:9000"   # RPC
    depends_on: [zk1, zk2, zk3, jn1, jn2, jn3]
    volumes:
      - nn1_data:/hadoop/dfs/name
    command: |
      bash -c "
        service ssh start ;
        echo 'Chờ JournalNode sẵn sàng...' ;
        sleep 15 ;
        if [ ! -f /hadoop/dfs/name/current/VERSION ]; then
          echo 'Format NameNode lần đầu...' ;
          hdfs namenode -format -clusterId mycluster-1 -force -nonInteractive ;
        fi ;
        echo 'Khởi tạo ZooKeeper failover controller...' ;
        hdfs zkfc -formatZK -force ;
        hdfs namenode &
        hdfs zkfc
      "
    healthcheck:
      test: ["CMD", "curl", "-f", "http://namenode1:9870"]
      interval: 30s
      timeout: 10s
      retries: 10

# ─────────────────────────────────────────────
# NAMENODE 2 — Standby
# ─────────────────────────────────────────────

  namenode2:
    build:
      context: .
      dockerfile: Dockerfile.hadoop
    container_name: namenode2
    hostname: namenode2
    networks: [hdfs-ha-net]
    ports:
      - "9871:9870"   # Web UI NN2
    depends_on:
      namenode1:
        condition: service_healthy
    volumes:
      - nn2_data:/hadoop/dfs/name
    command: >
      bash -c "
        service ssh start &&
        if [ ! -f /hadoop/dfs/name/current/VERSION ]; then
          echo 'Bootstrap Standby NameNode từ Active...' &&
          hdfs namenode -bootstrapStandby -force -nonInteractive
        fi &&
        hdfs namenode &
        hdfs zkfc
      "

# ─────────────────────────────────────────────
# DATANODE
# ─────────────────────────────────────────────

  datanode1:
    build:
      context: .
      dockerfile: Dockerfile.hadoop
    container_name: datanode1
    hostname: datanode1
    networks: [hdfs-ha-net]
    depends_on:
      namenode1:
        condition: service_healthy
    volumes:
      - dn1_data:/hadoop/dfs/data
    command: >
      bash -c "service ssh start && hdfs datanode"

  datanode2:
    build:
      context: .
      dockerfile: Dockerfile.hadoop
    container_name: datanode2
    hostname: datanode2
    networks: [hdfs-ha-net]
    depends_on:
      namenode1:
        condition: service_healthy
    volumes:
      - dn2_data:/hadoop/dfs/data
    command: >
      bash -c "service ssh start && hdfs datanode"

# ─────────────────────────────────────────────
# VOLUMES
# ─────────────────────────────────────────────

volumes:
  zk1_data:
  zk2_data:
  zk3_data:
  jn1_data:
  jn2_data:
  jn3_data:
  nn1_data:
  nn2_data:
  dn1_data:
  dn2_data:
```

> **Các điểm khác biệt quan trọng so với cấu hình đơn giản:**
>
> **ZooKeeper:** Dùng `image: zookeeper:3.8` trực tiếp thay vì build custom Dockerfile. File `zoo.cfg` được mount vào `/conf/zoo.cfg:ro` — đây là cách duy nhất đảm bảo `clientPort=2181` được áp dụng trong ZooKeeper 3.8.
>
> **namenode1 command:** Dùng `|` (literal block) thay vì `>` (folded block) trong YAML. Nếu dùng `>`, YAML gộp tất cả dòng thành một dòng khiến `hdfs zkfc -formatZK -force && hdfs namenode &` được bash parse thành `(formatZK && namenode) &` — cả hai chạy background, `hdfs zkfc` chạy ngay lập tức trước khi znode kịp tạo. Dùng `;` thay `&&` để tránh short-circuit ngoài ý muốn.
>
> **namenode1 healthcheck:** Dùng `http://namenode1:9870` thay vì `http://localhost:9870` vì Hadoop bind Web UI theo hostname, không phải `localhost`.
>
> **hdfs zkfc -formatZK:** Chạy **ngoài** khối `if`, nghĩa là luôn chạy mỗi lần namenode1 khởi động. Flag `-force` giúp lệnh idempotent — nếu znode đã tồn tại thì ghi đè, không lỗi. Điều này cần thiết vì nếu ZooKeeper bị restart (mất dữ liệu znode) trong khi volume namenode vẫn còn, khối `if` sẽ bị bỏ qua và zkfc sẽ không tìm thấy znode.

---

## Bước 5: Khởi Động Cluster

### 5.1 Build tất cả image Hadoop

```bash
docker-compose build
```

> ZooKeeper dùng image sẵn có, không cần build.

### 5.2 Khởi động theo thứ tự

Thứ tự khởi động **rất quan trọng** trong HA cluster:

```bash
# Bước 1: ZooKeeper quorum trước
docker-compose up -d zk1 zk2 zk3
sleep 10

# Kiểm tra ZooKeeper đã nhận kết nối chưa
docker exec zk1 sh -c "echo ruok | nc localhost 2181"
# Kết quả mong đợi: imok

# Bước 2: JournalNode quorum
docker-compose up -d jn1 jn2 jn3
sleep 15

# Bước 3: NameNode 1 (Active) — format + khởi tạo ZK
docker-compose up -d namenode1

# Chờ NameNode 1 healthy (có thể mất 1-2 phút)
docker-compose logs -f namenode1
# Ctrl+C khi thấy "Successfully transitioned NameNode ... to active state"

# Bước 4: NameNode 2 (Standby) — bootstrap từ Active
# namenode2 tự chờ namenode1 healthy nhờ depends_on condition
docker-compose up -d namenode2

# Bước 5: DataNode
docker-compose up -d datanode1 datanode2
```

### 5.3 Kiểm tra tất cả container đang chạy

```bash
docker-compose ps
```

Output mong đợi — tất cả **Up**:

```
NAME        STATUS             PORTS
zk1         Up
zk2         Up
zk3         Up
jn1         Up
jn2         Up
jn3         Up
namenode1   Up (healthy)       0.0.0.0:9870->9870, 0.0.0.0:9000->9000
namenode2   Up                 0.0.0.0:9871->9870
datanode1   Up
datanode2   Up
```

---

## Bước 6: Xác Nhận HA Hoạt Động

### 6.1 Web UI

| URL | Nội dung |
|---|---|
| http://localhost:9870 | NameNode 1 — phải hiển thị **active** |
| http://localhost:9871 | NameNode 2 — phải hiển thị **standby** |

### 6.2 Kiểm tra qua CLI

```bash
docker exec -it namenode1 bash

# Xem trạng thái HA
hdfs haadmin -getAllServiceState
```

Kết quả mong đợi:

```
namenode1/namenode1:9000    active
namenode2/namenode2:9000    standby
```

### 6.3 Kiểm tra ZooKeeper

```bash
docker exec -it zk1 bash

# Kết nối vào ZK CLI
zkCli.sh -server zk1:2181

# Xem node HA trong ZooKeeper
ls /hadoop-ha/mycluster
# Output: [ActiveBreadCrumb, ActiveStandbyElectorLock]

# Xem ai đang là Active
get /hadoop-ha/mycluster/ActiveBreadCrumb
```

---

## Bước 7: Test Failover Tự Động

Đây là bài kiểm tra quan trọng nhất — tắt Active NameNode và xem Standby có tự lên thay không.

### 7.1 Upload file thử trước

```bash
docker exec -it namenode1 bash
hdfs dfs -mkdir -p /test
echo "Test HA failover" > /tmp/ha-test.txt
hdfs dfs -put /tmp/ha-test.txt /test/
hdfs dfs -ls /test/
```

### 7.2 Kill namenode1 (đang Active)

Mở terminal mới:

```bash
docker-compose stop namenode1
```

### 7.3 Quan sát namenode2 tự động lên Active

```bash
docker-compose logs -f namenode2
```

Chờ khoảng **30–60 giây**, sẽ thấy log:

```
namenode2 ... Transitioning to active state
namenode2 ... STATE* Entering active state
```

Kiểm tra Web UI tại http://localhost:9871 — lúc này phải thấy **active**.

### 7.4 Dữ liệu vẫn còn nguyên

```bash
docker exec -it namenode2 bash
hdfs dfs -cat /test/ha-test.txt
# Output: Test HA failover  ✅
```

### 7.5 Khôi phục namenode1 về Standby

```bash
docker-compose start namenode1
# namenode1 tự nhận trạng thái standby
```

---

## Thao Tác HDFS Thường Dùng

```bash
docker exec -it namenode1 bash

# Báo cáo cluster
hdfs dfsadmin -report

# Kiểm tra replication của file
hdfs fsck /test/ha-test.txt -files -blocks -locations

# Thao tác file
hdfs dfs -mkdir -p /user/data
hdfs dfs -put /etc/hosts /user/data/
hdfs dfs -ls /user/data/
hdfs dfs -get /user/data/hosts /tmp/
hdfs dfs -rm /user/data/hosts

# Xem dung lượng
hdfs dfs -df -h /

# Chuyển failover thủ công (khi cần maintenance)
hdfs haadmin -failover nn1 nn2
```

---

## Quản Lý Cluster

```bash
# Dừng toàn bộ (giữ dữ liệu)
docker-compose stop

# Khởi động lại
docker-compose start

# Xem log theo dõi
docker-compose logs -f namenode1
docker-compose logs -f namenode2

# Xóa hoàn toàn kể cả dữ liệu
docker-compose down -v
```

---

## Xử Lý Lỗi Thường Gặp

### ❌ `JAVA_HOME does not exist`

**Nguyên nhân:** `hadoop-env.sh` hardcode đường dẫn Java theo kiến trúc CPU cụ thể (vd: `amd64`) nhưng máy đang chạy kiến trúc khác (vd: `arm64`).

**Giải pháp:** Dùng dynamic detection trong `hadoop-env.sh`:
```bash
export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java))))
```
Sau đó rebuild image: `docker-compose build`

---

### ❌ ZooKeeper `plain=disabled` — không nhận kết nối port 2181

**Nguyên nhân:** ZooKeeper 3.8 Docker image không xử lý biến môi trường `ZOO_PORT` để tạo `clientPort` một cách tin cậy.

**Giải pháp:** Mount file `zoo.cfg` trực tiếp vào container:
```yaml
volumes:
  - ./config/zoo.cfg:/conf/zoo.cfg:ro
```
Đảm bảo `zoo.cfg` có dòng `clientPort=2181` và `dataDir=/data`.

---

### ❌ `Parent znode does not exist` khi namenode1 khởi động

**Nguyên nhân 1:** ZooKeeper bị restart sau khi namenode đã format — znode bị mất nhưng `if [ ! -f VERSION ]` bỏ qua bước `hdfs zkfc -formatZK`.

**Nguyên nhân 2:** Lệnh namenode1 dùng `>` trong YAML khiến bash parse sai: `hdfs zkfc -formatZK -force && hdfs namenode &` được hiểu là `(formatZK && namenode) &` chạy background, sau đó `hdfs zkfc` chạy ngay lập tức trước khi znode kịp tạo.

**Giải pháp:** Dùng `|` (literal block) trong YAML và đặt `hdfs zkfc -formatZK -force` ngoài khối `if`, dùng `;` thay `&&`.

---

### ❌ namenode2 chờ mãi không start

**Nguyên nhân:** `namenode2` phụ thuộc `namenode1: condition: service_healthy`, mà healthcheck dùng `http://localhost:9870` — Hadoop bind Web UI theo hostname nên `localhost` không respond.

**Giải pháp:** Sửa healthcheck thành:
```yaml
healthcheck:
  test: ["CMD", "curl", "-f", "http://namenode1:9870"]
```

---

### ❌ `namenode2` không bootstrap được từ `namenode1`

**Nguyên nhân:** `namenode1` chưa hoàn toàn ở trạng thái Active khi `namenode2` khởi động.

**Giải pháp:**
```bash
docker-compose stop namenode2
docker-compose rm -f namenode2
docker volume rm hdfs-ha-docker_nn2_data
docker-compose up -d namenode2
```

---

### ❌ Cả 2 NameNode đều ở trạng thái Standby

**Nguyên nhân:** ZooKeeper chưa bầu chọn được leader, hoặc ZKFC không kết nối được ZK.

**Giải pháp:**
```bash
# Kiểm tra ZK quorum
docker exec zk1 sh -c "echo ruok | nc localhost 2181"   # Trả về "imok" là OK
docker exec zk2 sh -c "echo ruok | nc localhost 2181"
docker exec zk3 sh -c "echo ruok | nc localhost 2181"

# Nếu ZK OK, force elect nn1 làm Active
docker exec -it namenode1 bash
hdfs haadmin -transitionToActive --forcemanual nn1
```

---

### ❌ JournalNode log báo `Unable to start log segment`

**Nguyên nhân:** JournalNode khởi động trước khi NameNode format xong.

**Giải pháp:** Xóa dữ liệu journal và khởi động lại theo đúng thứ tự.
```bash
docker-compose down -v
docker-compose up -d zk1 zk2 zk3 && sleep 10
docker-compose up -d jn1 jn2 jn3 && sleep 15
docker-compose up -d namenode1
```

---

### ❌ Failover không xảy ra sau khi tắt Active NameNode

**Nguyên nhân:** ZKFC chưa detect được NameNode chết (timeout mặc định ~30s).

**Giải pháp:** Đây là bình thường — chờ thêm 30–60 giây. Hoặc cấu hình thêm vào `hdfs-site.xml`:
```xml
<property>
  <name>dfs.ha.zkfc.nn.zkTimeout</name>
  <value>10000</value>   <!-- milliseconds, giảm xuống để detect nhanh hơn -->
</property>
```

---

## Tổng Kết

| Thành phần | Số lượng | Chịu lỗi |
|---|---|---|
| ZooKeeper | 3 | Chịu được 1 node chết |
| JournalNode | 3 | Chịu được 1 node chết |
| NameNode | 2 | Active/Standby, tự động failover |
| DataNode | 2 | Replication factor = 2 |

**Cluster này đảm bảo:**
- ✅ Không có **single point of failure** ở tầng metadata
- ✅ **Automatic failover** trong vòng ~30–60 giây khi Active NameNode chết
- ✅ Dữ liệu không bị mất nhờ JournalNode quorum đồng bộ edit log liên tục
- ✅ Client **trong suốt** — tự tìm Active NameNode qua `ConfiguredFailoverProxyProvider`

---

*HDFS 3.4.3 | ZooKeeper 3.8 | Docker Compose v3.8 | Java 11*
