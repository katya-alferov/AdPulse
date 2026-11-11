<div dir="rtl">

# 🎯 הכנה לראיון טלפוני – Software Engineer Intern

> **כלל הכנות (הכי חשוב בכל המסמך):** אל תגידי שיש לך ניסיון מקצועי ב-Kafka/Spark/Docker. תגידי את האמת – והאמת חזקה:
> *"את Kafka, Spark ו-Docker למדתי דרך פרויקט אישי שבניתי לבד, מקצה לקצה, כדי ללמוד בידיים. הניסיון המקצועי שלי הוא ב-SQL, ETL, Informatica, DWH, Oracle ו-MSSQL – והפרויקט עזר לי לחבר את העולם הזה לעולם ה-streaming."*
> מראיינים מעריכים מאוד מועמדת שיודעת בדיוק מה היא יודעת ומה לא.

איך להשתמש במסמך: קראי בקול. לכל שאלה – תעני בעצמך קודם, ואז השווי. התשובות בנויות כך:
**🟢 פשוט** · **🔵 טכני** · **🟣 ב-AdPulse** · **🎯 מה המראיין רוצה לשמוע**

---

## חלק 1: הצגת הפרויקט

### ⏱️ 30 שניות (Elevator pitch)
> "בניתי לבד פרויקט בשם AdPulse – פלטפורמת אנליטיקה לפרסום בזמן אמת, בהשראת מה שחברת AdTech עושה. מחולל ב-Python מייצר חשיפות, קליקים והמרות – כולל כפילויות, אירועים שבורים, אירועים מאחרים ובוטים. האירועים עוברים דרך Kafka, Spark Structured Streaming מנקה אותם, מסיר כפילויות, מחשב CTR בחלונות של דקה ומזהה הונאת קליקים, ושומר ל-Parquet באחסון S3 ול-PostgreSQL. מעל זה יש FastAPI ודשבורד ב-Streamlit. הכול רץ ב-Docker Compose, עם בדיקות יחידה, בדיקות Spark ובדיקות end-to-end."

### ⏱️ 2 דקות
> "יש לי רקע ב-SQL, ETL ו-DWH – Informatica, Oracle, MSSQL. רציתי להבין איך עובדים עם נתונים שלא מגיעים פעם ביום ב-batch, אלא זורמים כל הזמן, אז בניתי פרויקט שמדמה חברת פרסום.
>
> **הבעיה:** חברת פרסום מקבלת מיליוני אירועים, וצריכה לספור אותם נכון – כי זה כסף. קליק שנספר פעמיים זה חיוב כפול למפרסם; קליקים של בוט זה הונאה; ומפרסם רוצה לראות עכשיו אם הקמפיין עובד.
>
> **הזרימה:** מחולל Python שולח JSON ל-Kafka, ל-topic עם 3 partitions, כשהמפתח הוא ה-user_id – כדי שאירועים של אותו משתמש יישארו בסדר. יש consumer group ב-Python שבודק תקינות ושולח הודעות פגומות ל-Dead Letter Queue. במקביל Spark Structured Streaming קורא את אותו topic: מפענח לפי schema, מוודא תקינות, מצרף את טבלאות הקמפיינים עם broadcast join, ומשתמש ב-watermark של 10 דקות כדי לקבל אירועים מאחרים בלי שהזיכרון יגדל לנצח. אחרי dedup לפי event_id הוא מחשב מדדים בחלונות של דקה ומזהה משתמש שלוחץ 8 פעמים בדקה ומעלה.
>
> **אחסון:** האירועים הגולמיים נשמרים כ-Parquet ב-object storage תואם S3 – data lake – והסיכומים נכנסים ל-PostgreSQL עם upsert אידמפוטנטי. יש גם job של batch שמדרג publishers בכל קמפיין עם window function.
>
> **מה למדתי:** הכי מעניין היה ההבדל בין event time ל-processing time, ולמה צריך watermark. ויש לי באג אמיתי שתיקנתי: כתיבה ל-S3 שנכשלה בגלל rename שלא קיים באחסון אובייקטים."

### ⏱️ 5 דקות – ארכיטקטורה (עם "ציור באוויר")
דברי לפי השכבות, ובכל שכבה: **מה, למה, trade-off**.
1. **Ingestion – Generator + Kafka.** למה Kafka ולא קריאה ישירה ל-API? Decoupling ו-buffer: אם Spark למטה, האירועים מחכים ב-Kafka (retention יום). 3 partitions = מקביליות; key=user_id = סדר per-user. Producer עם `acks=all` + idempotence.
2. **Validation – שתי שכבות.** Auditor ב-Python (consumer group, commit ידני אחרי עיבוד = at-least-once, DLQ). Spark עם אותם כללים כ-`CASE WHEN`. Trade-off: הכללים כתובים פעמיים; בפרודקשן הייתי שמה schema registry / data contract.
3. **Processing – Spark Structured Streaming.** micro-batch כל 20 שניות. parse → validate → broadcast join → watermark 10 דק' → `dropDuplicatesWithinWatermark` → window של דקה. 6 queries, לכל אחד checkpoint משלו. Trade-off של ה-watermark: גדול יותר = יותר אירועים מאחרים נספרים, אבל יותר state בזיכרון ותוצאות "סופיות" מאוחר יותר.
4. **Storage – שני סוגים.** Data lake (Parquet ב-S3, partitioned by date/type) לכל ההיסטוריה ולעיבוד מחדש; PostgreSQL לשאילתות מהירות של הדשבורד. Upsert של הערך המלא של החלון → replay אחרי קריסה לא משנה תוצאה.
5. **Serving – FastAPI + Streamlit.** הדשבורד לא ניגש ל-DB – רק ל-API. Repository pattern, פרמטרים ב-SQL, ולידציה של קלט, versioning `/api/v1`.
6. **Infra – Docker Compose.** 11 services, healthchecks, depends_on עם conditions, volumes, סודות ב-`.env`.
7. **Scale (מה הייתי משנה בפרודקשן):** Kafka 3 brokers עם replication 3; Spark על cluster (K8s/EMR); S3 אמיתי עם IAM roles; Postgres → מחסן עמודתי (ClickHouse/BigQuery/Vertica) לכמויות גדולות; monitoring (Prometheus/Grafana) על consumer lag; exactly-once עם sink טרנזקציוני.

---

## חלק 2: 12 השוואות קריטיות

### 1. Kafka מול Spark
- 🟢 Kafka = **הדואר** (מעביר ושומר הודעות). Spark = **המוח** (מחשב עליהן).
- 🔵 Kafka: מערכת אחסון והעברה של streams (log מבוזר, durable, pub/sub). לא מחשבת אגרגציות מורכבות. Spark: מנוע חישוב מבוזר (batch + streaming), לא שומר נתונים לאורך זמן בעצמו.
- 🟣 Generator → **Kafka** (buffer ל-24 שעות) → **Spark** (ניקוי, חלונות, הונאה).
- 🎯 שהם **משלימים** ולא מתחרים. בונוס: יש Kafka Streams לעיבוד פשוט בתוך Kafka.

### 2. Spark מול Pandas
- 🟢 Pandas = מחשבון חזק על השולחן שלך. Spark = מפעל עם 100 מחשבונים.
- 🔵 Pandas: מחשב אחד, הכול בזיכרון, eager (מבצע מיד), נהדר עד כמה GB. Spark: מבוזר, lazy, אופטימייזר, streaming, עמיד לנפילות; overhead גבוה לנתונים קטנים.
- 🟣 ב-Spark מעבדים את הזרם; בדשבורד (נתונים מסוכמים וקטנים) – Pandas.
- 🎯 לדעת **מתי לא** להשתמש ב-Spark: 100MB? Pandas מהיר יותר.

### 3. Kafka מול REST API
- 🟢 REST = טלפון (צריך שמישהו יענה עכשיו). Kafka = תיבת דואר (משאירים הודעה).
- 🔵 REST: request/response סינכרוני, point-to-point, אם השרת למטה – נכשל. Kafka: אסינכרוני, הרבה צרכנים לאותה הודעה, durable, replay.
- 🟣 אירועים → Kafka. דשבורד ↔ FastAPI.
- 🎯 לבחור לפי הצורך: צריך תשובה מיידית → REST; זרם אירועים / decoupling → Kafka.

### 4. Docker מול Virtual Machine
- 🟢 VM = בית שלם עם יסודות משלו. Container = דירה בבניין (חולקים יסודות, כל אחד עם דלת משלו).
- 🔵 VM: hypervisor, מערכת הפעלה מלאה לכל VM, GBs, דקות. Container: חולק kernel של ה-host, מבודד עם namespaces/cgroups, MBs, שניות. VM מבודדת יותר.
- 🟣 11 קונטיינרים על לפטופ אחד – עם VMs זה לא היה אפשרי.
- 🎯 shared kernel, image vs container, למה זה נותן "works on my machine → works everywhere".

### 5. Batch מול Streaming
- 🟢 כביסה פעם בשבוע מול מדיח שמנקה כל צלחת.
- 🔵 Batch: נתונים סופיים, latency גבוה, פשוט יותר, קל לחזור על ריצה. Streaming: אינסופי, latency נמוך, צריך state, watermarks, checkpoints, טיפול באיחורים.
- 🟣 `streaming_job.py` (מדדים כל 20 שניות) מול `batch_report_job.py` (דירוג מכל ה-Parquet).
- 🎯 שהרבה מערכות משלבות (Lambda/Kappa architecture). ה-ETL שלי ב-Informatica היה batch – וזה מה שרציתי להשלים.

### 6. SQL מול PySpark
- 🟢 אותה שפה, שני מבטאים.
- 🔵 שניהם declarative ועוברים אופטימייזר (Spark SQL ו-DataFrame API מתורגמים לאותה תוכנית!). SQL ב-DB: נתונים בתוך ה-DB. PySpark: קבצים/זרמים מבוזרים, אפשר לשלב Python, קל לבדיקות יחידה ולמחזור קוד.
- 🟣 `SUM(CASE WHEN event_type='click' THEN 1 ELSE 0 END)` = `F.sum(F.when(F.col("event_type")=="click",1).otherwise(0))`. `RANK() OVER (PARTITION BY ...)` = `F.rank().over(Window.partitionBy(...))`.
- 🎯 שהידע שלי ב-SQL עובר ישירות. (נקודת חוזק שלך!)

### 7. S3 מול PostgreSQL
- 🟢 מחסן ענק וזול מול ספרייה עם קטלוג.
- 🔵 S3: object storage, זול, אינסופי, עמיד, אין אינדקסים/טרנזקציות על שורות, מתאים לקבצים גדולים וסריקות. Postgres: רלציוני, ACID, אינדקסים, שאילתות נקודתיות במילישניות, יקר יותר לנפח.
- 🟣 כל האירועים הגולמיים → Parquet ב-S3. סיכומים לדקה → Postgres.
- 🎯 data lake vs serving layer/data mart.

### 8. Threads מול Processes
- 🟢 Process = משפחה בבית משלה. Threads = בני משפחה באותו בית (חולקים מטבח = זיכרון).
- 🔵 Process: זיכרון נפרד, בידוד, תקשורת יקרה (IPC). Thread: זיכרון משותף, זול, אבל צריך סנכרון (race conditions, locks). ב-Python: **GIL** – רק thread אחד מריץ bytecode בכל רגע → threads טובים ל-I/O, processes (multiprocessing) ל-CPU.
- 🟣 Spark `local[*]` = thread לכל ליבה בתוך JVM אחד. librdkafka שולח ברקע ב-thread משלו (לכן `poll()`). כל קונטיינר = process נפרד.
- 🎯 GIL, I/O-bound מול CPU-bound.

### 9. Transformation מול Action
- 🟢 לכתוב רשימת קניות מול ללכת לסופר.
- 🔵 Transformations (lazy): `select/filter/groupBy/join` – בונות DAG. Actions: `count/collect/show/write/start` – מפעילות. Narrow vs wide (shuffle).
- 🟣 כל `transformations.py` = transformations. `.start()` ב-streaming_job = action.
- 🎯 שאפשר להסביר **למה** lazy טוב (אופטימיזציה של כל התוכנית) ומה **מסוכן** (`collect()` על נתונים גדולים מפיל את ה-driver).

### 10. Producer מול Consumer
- 🟢 שולח מכתבים מול קורא מכתבים.
- 🔵 Producer: בוחר partition (לפי key), batching, acks, retries, idempotence. Consumer: poll, group membership, offsets, commit, rebalance.
- 🟣 `AdEventProducer` (acks=all, key=user_id); `EventAuditor` (group, commit ידני, DLQ).
- 🎯 delivery semantics: at-most / at-least / exactly-once.

### 11. Partition מול Consumer Group
- 🟢 Partitions = תאים במגירה. Consumer group = צוות שמתחלק בתאים.
- 🔵 Partition = יחידת מקביליות ו-ordering. בקבוצה – partition אחד ↔ consumer אחד לכל היותר. יותר consumers מ-partitions → חלק מובטלים. קבוצות שונות = קריאה עצמאית.
- 🟣 3 partitions → עד 3 auditors יעילים. Spark ו-auditor קוראים את אותם נתונים בלי להפריע.
- 🎯 "מקסימום מקביליות = מספר ה-partitions".

### 12. Event Time מול Processing Time
- 🟢 התאריך על הגלויה מול יום שהיא הגיעה.
- 🔵 Event time נכון עסקית אבל דורש טיפול באיחורים (watermark, state). Processing time פשוט אבל משקר כשיש עיכובים.
- 🟣 חלונות לפי `event_ts`; 3% מהאירועים מאחרים 1–14 דק'; watermark 10 דק'; בדיקה שמוכיחה שאירוע מ-10:00 נזרק אחרי שראינו 10:20, ו-10:15 נספר.
- 🎯 watermark = trade-off בין שלמות לבין latency וזיכרון.

---

## חלק 3: 34 שאלות טכניות עם תשובות

### 🐍 Python

**1. מה ההבדל בין list ל-tuple? מתי dict?**
🟢 list = קופסה שאפשר לשנות; tuple = קופסה סגורה; dict = ארון עם מגירות עם שמות.
🔵 list mutable, tuple immutable (ולכן hashable – יכול להיות key ב-dict). dict – hash table, חיפוש O(1) בממוצע.
🟣 `REQUIRED_FIELDS` הוא tuple (קבוע); `CAMPAIGNS_BY_ID` הוא dict לחיפוש מהיר; `Counter` (dict) לספירת שגיאות.
🎯 mutability ו-complexity.

**2. מה זה generator / yield?**
🔵 פונקציה שמחזירה ערכים אחד-אחד, lazy, בלי להחזיק הכול בזיכרון.
🟣 `EventFactory.stream()` – `yield from self.next_batch()` – זרם אינסופי בלי לבנות רשימה אינסופית.
🎯 זיכרון O(1) לזרמים.

**3. מה זה decorator?**
🔵 פונקציה שעוטפת פונקציה ומוסיפה התנהגות.
🟣 `@dataclass`, `@app.get(...)` (רושם route), `@lru_cache` (singleton של repository), `@st.fragment(run_every=10)`.

**4. מה זה dataclass ולמה frozen?**
🔵 יוצר `__init__/__repr__/__eq__` אוטומטית. `frozen=True` → immutable.
🟣 `Settings` frozen – אף אחד לא ישנה הגדרות באמצע ריצה. `field(repr=False)` – סיסמה לא נכנסת ללוג.

**5. איך מטפלים בשגיאות נכון?**
🔵 לתפוס שגיאות **ספציפיות**, להוסיף הקשר, לא לבלוע בשקט, `finally` לניקוי, `raise ... from exc` לשמירת הסיבה.
🟣 `except BufferError` ב-producer (back-pressure); `except KafkaException` עם retries ב-admin; `finally: consumer.close()`; `ConfigError ... from exc`.
🎯 לא `except: pass`.

**6. מה זה type hints? הם נאכפים?**
🔵 לא בזמן ריצה (חוץ מספריות כמו Pydantic/FastAPI שמשתמשות בהם לולידציה). עוזרים ל-IDE, mypy, וקריאות.
🟣 FastAPI מוודא ש-`minutes: int = Query(ge=1)` → `?minutes=abc` מחזיר 422.

### 🗄️ SQL

**7. מה ההבדל בין WHERE ל-HAVING?**
WHERE מסנן שורות **לפני** GROUP BY; HAVING מסנן קבוצות **אחרי**. 🟣 זיהוי הונאה = `GROUP BY user, minute HAVING COUNT(*) >= 8` (ב-Spark: `groupBy().agg().filter()`).

**8. INNER מול LEFT JOIN – ומה הסכנה?**
INNER מחזיר רק התאמות; LEFT שומר את כל השמאלי. 🟣 בחרתי LEFT בין אירועים לקמפיינים: INNER היה **מעלים בשקט** אירועים עם קמפיין לא מוכר; עם LEFT אני מסמנת אותם `unknown_campaign` וסופרת. 🎯 מודעות לאובדן נתונים שקט.

**9. מה זה window function? תני דוגמה.**
חישוב על "חלון" של שורות בלי לכווץ אותן (בניגוד ל-GROUP BY). `RANK() OVER (PARTITION BY campaign_id ORDER BY clicks DESC)`. 🟣 batch_report_job. גם: `LAG` לשינוי מדקה לדקה, `SUM() OVER (ORDER BY ...)` לסכום מצטבר.

**10. איך מונעים חלוקה באפס ב-CTR?**
`clicks * 100.0 / NULLIF(impressions, 0)` → NULL במקום שגיאה. שימי לב ל-`100.0` – אחרת חלוקת integers נותנת 0.

**11. מה זה UPSERT ולמה הוא חשוב ב-streaming?**
`INSERT ... ON CONFLICT (key) DO UPDATE` (MERGE באורקל/MSSQL). 🟣 Spark שולח את הערך העדכני של החלון; upsert של **ערך מלא** = אידמפוטנטי: replay אחרי קריסה לא מכפיל.

**12. מתי אינדקס עוזר ומתי מזיק?**
עוזר לחיפוש/סינון/מיון על עמודות סלקטיביות; מאט כתיבות ותופס מקום. 🟣 `(campaign_id, window_start)` לשאילתות timeseries לפי קמפיין.

### 📨 Kafka

**13. איך Kafka מבטיח סדר?**
רק **בתוך partition**. לכן key: כל האירועים של `user_id` מסוים באותו partition. 🎯 אין סדר גלובלי בין partitions.

**14. מה זה consumer lag ולמה מנטרים אותו?**
ההפרש בין ההודעה האחרונה ל-offset שה-consumer הגיע אליו. lag שגדל = הצרכן לא עומד בקצב → צריך עוד consumers (עד מספר ה-partitions) או לייעל. 🟣 `kafka-consumer-groups.sh --describe`.

**15. at-least-once / at-most-once / exactly-once?**
commit אחרי עיבוד → at-least-once (כפילויות אפשריות); לפני → at-most-once (אובדן אפשרי); exactly-once → idempotent producer + transactions + sink טרנזקציוני/אידמפוטנטי. 🟣 בחרתי at-least-once + **dedup לפי event_id + upsert אידמפוטנטי** = תוצאה "effectively once" למדדים.

**16. מה קורה כשמוסיפים consumer לקבוצה?**
Rebalance – Kafka מחלק מחדש partitions. במהלכו הצריכה נעצרת לרגע. Consumer שסוגר נכון (`close()`) מקצר את זה.

**17. מה זה DLQ ולמה לא פשוט לזרוק הודעות פגומות?**
Topic להודעות שנכשלו בעיבוד. לא זורקים כי: אפשר לחקור (באג אצל המפיק?), לתקן ולהריץ מחדש, ולמדוד איכות. 🟣 `ad-events-dlq` עם reason + partition + offset מקוריים.

**18. למה acks=all ו-idempotence?**
acks=all – לא מאבדים הודעה אם leader נופל אחרי אישור. idempotence – retry לא יוצר כפילות ב-log. מחיר: latency מעט גבוה יותר.

### ⚡ Spark

**19. מה זה watermark ומה קורה בלעדיו?**
"כמה זמן אחכה לאיחורים". בלי watermark – Spark לא יכול לסגור חלונות ולמחוק state → זיכרון גדל לנצח, ו-dedup זוכר כל event_id אי פעם.

**20. מה זה checkpoint?**
תיקייה שבה Spark שומר offsets שעובדו ו-state. מאפשר המשך אחרי קריסה בלי לאבד/לכפול. חייב להיות נפרד לכל query, ולא משנים query "שובר" (schema של state) עם אותו checkpoint.

**21. append מול update מול complete?**
append – רק שורות סופיות (עם watermark – אחרי שהחלון נסגר). update – שורות שהשתנו (מהיר לדשבורד). complete – כל התוצאה כל פעם. 🟣 Parquet=append, מדדים=update+upsert.

**22. מה זה shuffle ולמה הוא יקר?**
העברת נתונים בין executors לפי key (groupBy/join). כרוך בכתיבה לדיסק, רשת, serialization. מפחיתים עם broadcast join, סינון מוקדם, partitioning נכון. 🟣 `shuffle.partitions=4` ו-broadcast לטבלאות הקטנות.

**23. מה זה data skew ואיך מטפלים?**
key אחד עם הרבה יותר נתונים (קמפיין ענק) → task אחד איטי שמעכב הכול. פתרונות: salting (מפתח + מספר אקראי), AQE (Adaptive Query Execution) ב-Spark 3, broadcast.

**24. למה `collect()` מסוכן?**
מביא את כל הנתונים ל-driver → OutOfMemory. 🟣 אני משתמשת ב-`collect()` רק ב-foreachBatch על **סיכומים** (עשרות שורות).

**25. מה זה Parquet ולמה לא CSV?**
עמודתי, דחוס, עם schema וסטטיסטיקות → column pruning + predicate pushdown. CSV: טקסט, בלי סוגים, קוראים הכול. + partitioning בתיקיות.

### 🐳 Docker

**26. מה ההבדל בין image ל-container?**
image = תבנית read-only (שכבות). container = מופע רץ שלה עם שכבה כתיבה. 🟣 `adpulse-app:1.0.0` → 6 קונטיינרים שונים.

**27. למה volume?**
קונטיינר הוא זמני; נתונים בתוכו נמחקים איתו. Volume שורד. 🟣 `postgres-data`, `kafka-data`, `seaweedfs-data`, `spark-checkpoints`.

**28. depends_on מבטיח שהשירות מוכן?**
לא לבד! רק סדר הפעלה. צריך `condition: service_healthy` + healthcheck אמיתי (`pg_isready`). וגם retries בקוד – כי שירותים יכולים ליפול גם אחר כך.

**29. למה סדר השורות ב-Dockerfile משנה?**
Layer caching. `COPY requirements` + `pip install` לפני `COPY src` → שינוי קוד לא מתקין מחדש ספריות.

### ☁️ Cloud

**30. איך היית מעבירה את AdPulse ל-AWS?**
S3 (data lake), MSK (Kafka), EMR/Glue (Spark), RDS (Postgres), ECS Fargate (API/dashboard), IAM roles במקום מפתחות, Secrets Manager, CloudWatch. **הקוד כמעט לא משתנה** – רק endpoints ו-credentials. (ולהגיד בכנות: לא פרסתי לענן כי רציתי פרויקט חינמי לגמרי; זה ניתוח תיאורטי.)

**31. מה זה IAM ו-least privilege?**
מי מורשה לעשות מה על איזה משאב. least privilege = רק ההרשאות ההכרחיות (Spark צריך `s3:PutObject` רק על `adpulse-lake/*`, לא admin על כל החשבון).

### 🌐 REST

**32. אילו status codes ה-API שלך מחזיר?**
200 / 404 (קמפיין לא קיים) / 422 (פרמטר לא תקין, אוטומטי מ-FastAPI) / 503 (DB לא זמין, בלי לחשוף stack trace).

**33. איך היית מוסיפה pagination/caching?**
`limit/offset` או cursor (keyset) לרשימות גדולות; cache של כמה שניות לסיכום (ב-Redis או בזיכרון) כי הנתונים מתעדכנים כל 20 שניות ממילא.

### 🧱 Software Engineering

**34. איך בדקת את הפרויקט?**
פירמידת בדיקות: 59 unit (מהירות, עם fakes ו-dependency injection), 8 Spark (כולל stream אמיתי שמוכיח watermark), 7 integration על המערכת החיה (שולחת בוט עם מזהה ייחודי ומחכה שיופיע ב-DB). 🎯 testability מכוונת: `now` כפרמטר, seed, הזרקת תלויות.

---

## חלק 4: אלגוריתמים ומבני נתונים (קשור לפרויקט)

**א. מצאי את 3 הקמפיינים עם הכי הרבה קליקים מתוך מיליון אירועים.**
<div dir="ltr">

```python
from collections import Counter
import heapq
counts = Counter(e["campaign_id"] for e in events if e["event_type"] == "click")   # O(n)
top3 = heapq.nlargest(3, counts.items(), key=lambda kv: kv[1])                     # O(k log 3)
```

</div>
או פשוט `counts.most_common(3)`. 🎯 hash map + heap, לא מיון של הכול (O(n log n)).

**ב. הסירי כפילויות לפי event_id ושמרי סדר.**
<div dir="ltr">

```python
seen, result = set(), []
for e in events:
    if e["event_id"] not in seen:
        seen.add(e["event_id"]); result.append(e)
```

</div>
O(n) זמן, O(n) זיכרון. שאלת המשך: "ומה אם זה זרם אינסופי?" → לזכור רק בחלון זמן (בדיוק מה ש-watermark עושה), או Bloom filter (חוסך זיכרון, false positives אפשריים).

**ג. זהי משתמש עם ≥ 8 קליקים בכל חלון של 60 שניות (sliding).**
<div dir="ltr">

```python
from collections import defaultdict, deque
recent = defaultdict(deque)
def on_click(user, ts):
    q = recent[user]; q.append(ts)
    while q and q[0] <= ts - 60: q.popleft()
    return len(q) >= 8
```

</div>
deque כ-sliding window, O(1) amortized. (ב-AdPulse השתמשתי ב-tumbling window – פשוט יותר; sliding תופס גם burst שחוצה גבול דקה.)

**ד. מזגי שתי רשימות ממוינות של אירועים לפי זמן** – two pointers, O(n+m). (כמו מה ש-Kafka consumer עושה כשקורא מכמה partitions.)

**ה. Complexity בסיסי:** חיפוש ב-dict/set O(1); ברשימה O(n); מיון O(n log n); binary search O(log n).

---

## חלק 5: תרחישי דיבאג

**1. "הדשבורד מראה אפס קליקים כבר 10 דקות."**
עקבי אחרי הנתונים: generator שולח? → Kafka offsets עולים? → Spark UI: input rate? exceptions? → Postgres: `max(updated_at)`? → API מחזיר? → דשבורד. הראשון שנכשל – שם הבאג. (במקרה שלי: Spark נפל בלולאה בגלל כתיבה ל-S3.)

**2. "CTR קפץ פתאום ל-40%."**
חשד לבוטים או כפילויות. לבדוק `suspicious_activity` ו-`duplicate_events`; לבדוק אם ה-dedup עובד (checkpoint נמחק?); לבדוק אם חשיפות **ירדו** (מכנה) – אולי באג במפיק החשיפות.

**3. "Consumer lag גדל כל הזמן."**
עוד consumers (עד מספר partitions), להגדיל partitions (זהירות – משנה מיפוי keys), לייעל עיבוד (batching), לבדוק data skew על partition אחד.

**4. "אחרי restart של Spark המספרים הוכפלו."**
checkpoint לא נשמר (אין volume?) → קרא מ-earliest מחדש; sink לא אידמפוטנטי (INSERT במקום UPSERT). אצלי: volume ל-checkpoints + upsert.

**5. "Spark job קורס עם OutOfMemory."**
`collect()` גדול? state גדל בלי watermark? skew? `maxOffsetsPerTrigger` חסר אחרי השבתה ארוכה (בולע הכול בבת אחת)?

**6. "השירות עובד אצלי ולא בקונטיינר."**
`localhost` בתוך קונטיינר = הקונטיינר עצמו! צריך שם שירות (`kafka:9092`). ובכיוון ההפוך – advertised listeners.

---

## חלק 6: Scalability, Distributed Systems, עקביות וכשלים

**"מה יקרה אם התנועה תגדל פי 1000?"**
- Kafka: יותר partitions (מקביליות) ו-brokers; replication 3.
- Spark: cluster עם יותר executors; partitions תואמים; trigger מותאם.
- Postgres יהפוך לצוואר בקבוק בכתיבה/שאילתות → מחסן עמודתי (ClickHouse/Druid/BigQuery), pre-aggregation, partitioning לפי זמן.
- API: כמה replicas מאחורי load balancer (הוא stateless!), cache.
- Data lake: compaction של קבצים קטנים (בעיית "small files"), פורמט טבלאי (Delta/Iceberg) ל-ACID ו-time travel.

**"מה קורה אם כל רכיב נופל?"**
| נופל | מה קורה | למה זה בסדר |
|---|---|---|
| Spark | אירועים נשמרים ב-Kafka | ממשיך מה-checkpoint |
| Postgres | foreachBatch נכשל → query נופל → restart | ה-batch ירוץ שוב; upsert אידמפוטנטי |
| Kafka | ה-producer מנסה שוב (retries), תור מקומי | בפרודקשן: replication |
| API | הדשבורד מציג שגיאה ידידותית | אין אובדן נתונים |
| Generator | אין אירועים חדשים | שאר המערכת בריאה |

**CAP בקצרה:** במערכת מבוזרת עם חלוקת רשת (Partition) צריך לבחור בין עקביות (Consistency) לזמינות (Availability). Kafka עם `acks=all` ו-`min.insync.replicas` מעדיף עקביות.

**Eventual consistency בפרויקט:** הדשבורד מפגר ב-20–30 שניות אחרי המציאות, וחלון "פתוח" עוד יכול להשתנות עד שה-watermark עובר. זה trade-off מודע.

---

## חלק 7: שאלות System Design על הארכיטקטורה

1. **למה לא לכתוב ישר מה-generator ל-Postgres?** אין buffer, אין replay, צימוד חזק, Postgres לא בנוי למיליוני INSERT בשנייה, ואין מקום ל-dedup/watermark.
2. **למה גם Parquet וגם Postgres?** מקור אמת גולמי שאפשר לעבד מחדש (באג בנוסחה? מחשבים מחדש מה-lake) מול שכבת הגשה מהירה.
3. **איך היית משנה את סף ההונאה בלי deploy?** משתנה סביבה כבר היום (`SUSPICIOUS_CLICK_THRESHOLD`); בהמשך – טבלת כללים, או מודל ML.
4. **איך היית מוסיפה סוג אירוע חדש (video_view)?** schema evolution: שדה אופציונלי, עדכון `VALID_EVENT_TYPES` בשני המקומות (→ סיבה ל-schema registry), עמודה חדשה באגרגציה, migration ב-SQL.
5. **איך היית מספקת exactly-once?** Kafka transactions + sink טרנזקציוני (למשל כתיבה עם batch_id שמור באותה טרנזקציה ב-Postgres, ודילוג על batch שכבר נכתב).
6. **איך היית מנטרת את המערכת?** consumer lag, input/processing rate ב-Spark, batch duration, שגיאות API, latency מקצה לקצה (sent_at → updated_at), כמות DLQ – עם Prometheus + Grafana (חינמיים) ו-alerts.
7. **איך היית מאבטחת?** TLS/SASL ב-Kafka, IAM roles, Secrets Manager, רשת פרטית, auth ל-API (OAuth/JWT), rate limiting, לא לחשוף Postgres לאינטרנט.

---

## חלק 8: שאלות שכדאי לך לשאול את המראיין
- "איך נראה ה-pipeline של הנתונים אצלכם – Kafka ו-Spark, או משהו אחר?"
- "מה האתגר הכי גדול שלכם בזיהוי קליקים לא תקינים?"
- "איך נראה היום הראשון/החודש הראשון של מתמחה בצוות?"
- "איך עושים code review ובדיקות אצלכם?"

## חלק 9: צ'קליסט לפני הראיון ✅
- [ ] אני יודעת לומר את ה-pitch של 30 שניות בלי להסתכל.
- [ ] אני יודעת לצייר את הארכיטקטורה ב-7 קופסאות.
- [ ] אני יודעת להסביר watermark עם דוגמת המורה והמבחנים.
- [ ] אני יודעת להסביר partition + key + ordering.
- [ ] אני יודעת לספר על הבאג של S3 rename ואיך פתרתי.
- [ ] אני יודעת להגיד בכנות מה למדתי בפרויקט ומה מהעבודה.
- [ ] הרצתי את הפרויקט בעצמי לפחות פעם אחת והסתכלתי על Spark UI.

</div>
