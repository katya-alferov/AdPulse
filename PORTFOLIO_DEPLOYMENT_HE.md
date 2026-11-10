# 🌍 פורטפוליו ציבורי ודמו חי – תוכנית חינמית

> **שום דבר לא פורסם.** המאגר פרטי, ואין דמו באוויר. כל צעד ציבורי כאן מחכה לאישור המפורש שלך.

## 1. השורה התחתונה
| רכיב | אפשר לאחסן בחינם בציבור? | ההחלטה |
|---|---|---|
| **דשבורד Streamlit (מצב דמו)** | ✅ כן – Streamlit Community Cloud | הדמו הציבורי |
| **קוד מלא + תיעוד + צילומי מסך** | ✅ כן – GitHub repo ציבורי | הפורטפוליו |
| Kafka + Spark + PostgreSQL + S3 | ❌ לא בחינם בצורה אמינה | נשארים להרצה מקומית ב-Docker |
| FastAPI | ⚠️ אפשרי טכנית, אבל בלי DB אין לו מה להגיש | לא נדרש בדמו |

**למה לא להעלות את הכול?** Spark צריך 2GB+ זיכרון ו-Java, Kafka צריך שרת שרץ כל הזמן. אין אירוח חינמי שמריץ 11 קונטיינרים כאלה 24/7 בלי כרטיס אשראי או בלי שזה יקרוס. "לדחוס" את זה לפלטפורמה חינמית היה נותן דמו שבור – רע יותר מכלום.

## 2. מה בדקתי (נובמבר 2026)

### Streamlit Community Cloud ✅ (נבחר)
- **מחיר:** חינמי. התחברות עם GitHub. לא ראיתי דרישה לכרטיס אשראי.
- **מגבלות (לפי התיעוד הרשמי, מספרים מפברואר 2024 שעשויים להשתנות):** 0.078–2 ליבות CPU, 690MB–2.7GB זיכרון, עד 50GB אחסון.
- **שינה:** אפליקציה בלי תנועה 12 שעות נרדמת. כל מבקר יכול להעיר אותה (זה לוקח כחצי דקה).
- **פרטיות:** מספר בלתי מוגבל של אפליקציות ציבוריות, ואפליקציה פרטית אחת.
- **מה צריך:** חשבון GitHub עם הרשאת admin על ה-repo. האירוח בארה"ב.
- המצב הדמו שלנו משתמש ב-~150MB זיכרון ובקובצי JSON של 230KB – הרבה מתחת למגבלות.

### Hugging Face Spaces ❌
- מיולי 2026 Spaces מסוג Docker/Gradio עברו לתוכנית בתשלום (PRO). רק Static Spaces נשארו חינמיים, ואין יותר תבנית Streamlit. לא מתאים.

### Render / Railway / Fly.io ❌
- חלק דורשים כרטיס אשראי, ובחלק השרת החינמי נכבה ומוגבל מאוד. לא נכנסים לזה, לפי הדרישה שלך לא להפעיל שום דבר בתשלום.

### AWS Free Tier ❌
- Free Tier לא אומר "חינם בלי הגבלה": MSK ו-EMR לא כלולים, ודורשים כרטיס אשראי. לא נוגעים.

## 3. הארכיטקטורה של הדמו

```
  ┌─────────────────── אצלך במחשב (Docker) ────────────────────┐
  │ Generator → Kafka → Spark → Postgres/S3 → FastAPI           │
  │                                    │                        │
  │              python tools/export_demo_data.py               │
  │                                    ▼                        │
  │                   demo_data/*.json (snapshot סינתטי)        │
  └────────────────────────────────┬────────────────────────────┘
                                   │ git push
                                   ▼
                    GitHub (repo ציבורי, באישורך)
                                   │
                                   ▼
        Streamlit Community Cloud מריץ streamlit_app.py
        → אותו קוד דשבורד, ADPULSE_MODE=demo, קורא את ה-JSON
```

- **אותו קוד דשבורד בדיוק.** ההבדל היחיד: מקור הנתונים (`DemoDataSource` במקום `AdPulseClient`).
- **ה-backend לא נחשף.** אין חיבור מהאינטרנט למחשב שלך – אין פתיחת פורטים, אין tunnel. הנתונים מגיעים מקובץ סטטי.
- הנתונים **אמיתיים מבחינת המערכת** (תוצאה של Spark על Kafka) ו**סינתטיים** מבחינת התוכן.

## 4. מה מבקרים יראו ומה לא

| ✅ מבקרים יוכלו | ❌ מבקרים לא יוכלו |
|---|---|
| לגלוש בכל 4 עמודי הדשבורד (KPI, גרפים, טבלאות, drill-down לקמפיין, טווחי זמן) | לראות נתונים מתעדכנים בזמן אמת (זה snapshot) |
| לראות את הקוד המלא של Kafka/Spark/Docker ב-GitHub | להפעיל את Kafka/Spark באתר |
| לקרוא את התיעוד, הארכיטקטורה ולראות צילומי מסך | לקרוא ל-REST API (הוא לא מאוחסן) |
| להריץ את **כל המערכת** אצלם במחשב בפקודה אחת | לגשת למחשב שלך או לסודות כלשהם |

בדשבורד הדמו יש תיבה שאומרת בבירור "Demo mode – synthetic snapshot exported from a real local run", ותאריך ה-export בתחתית.

## 5. צ'קליסט לפני שהופכים משהו לציבורי 🔒
- [ ] `git ls-files | grep -E "\.env$"` מחזיר כלום.
- [ ] `git log --all -p | grep -iE "password|secret|token|aws_"` – רק שמות משתנים וערכי דוגמה מ-`.env.example`.
- [ ] אין פרטים אישיים בקבצים (אימייל, טלפון). ה-commits חתומים עם כתובת `noreply` של GitHub.
- [ ] `demo_data/` מכיל רק נתונים סינתטיים (קמפיינים ומשתמשים מומצאים, IP מטווח פרטי 10.x).
- [ ] README מבהיר שזה פרויקט לימודי אישי, נתונים סינתטיים, לא קשור לאף חברה.
- [ ] החלטת שאת רוצה שה-repo יהיה ציבורי (אפשר גם להשאיר פרטי ולשתף קישור לדמו בלבד – Streamlit Cloud יכול לפרוס מ-repo פרטי; האפליקציה עצמה תהיה ציבורית).

## 6. צעדי פריסה (כשתאשרי)

**א. להפוך את ה-repo לציבורי (אופציונלי):**
GitHub → AdPulse → Settings → General → Danger Zone → Change visibility → Public.

**ב. לפרוס את הדמו:**
1. https://share.streamlit.io → Sign in with GitHub.
2. Create app → Deploy a public app from GitHub.
3. Repository: `katya-alferov/AdPulse` · Branch: `main` · Main file path: `streamlit_app.py`.
4. App URL: בחרי משהו כמו `adpulse-demo`.
5. Deploy. (Streamlit Cloud מתקין את `requirements.txt` שבשורש – רק streamlit, plotly, pandas, requests.)
6. אין צורך ב-Secrets – הדמו לא מתחבר לשום דבר.

**ג. לעדכן את ה-snapshot מדי פעם:**
```bash
docker compose up -d --build        # להריץ את המערכת המלאה ~15 דקות
docker compose run --rm spark-batch
python tools/export_demo_data.py --api http://localhost:8000 --out demo_data
git add demo_data && git commit -m "Refresh demo snapshot" && git push   # Streamlit Cloud מתעדכן לבד
```

**ד. להוסיף ל-README את הקישור:**
`**Live demo:** https://<your-app>.streamlit.app (synthetic snapshot – see "Dashboard demo mode")`

## 7. מה לכתוב בקורות חיים / LinkedIn (בכנות)
> **AdPulse – Real-time AdTech analytics (personal project)** · Python, Kafka, Spark Structured Streaming, Docker, PostgreSQL, FastAPI, Streamlit
> Built independently as a hands-on learning project: streaming pipeline with event-time windows, watermarking, deduplication and click-fraud detection; Parquet data lake on S3-compatible storage; 74 automated tests. [GitHub] · [Live demo]

## 8. איך מגייס מריץ את המערכת המלאה
מופיע ב-README תחת "Quick start" – שלוש פקודות: `git clone`, `cp .env.example .env`, `docker compose up -d --build`. אחרי ~2 דקות: http://localhost:8501.
