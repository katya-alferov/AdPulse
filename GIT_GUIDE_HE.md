# 🗂️ מדריך Git ו-GitHub – גיבוי AdPulse

## 1. מושגים, בפשטות

| מושג | הסבר לילדה בת 10 | הסבר טכני |
|---|---|---|
| **Git** | מכונת זמן לתיקייה שלך. בכל רגע אפשר לחזור לגרסה קודמת. | מערכת ניהול גרסאות מבוזרת. כל ההיסטוריה שמורה מקומית בתיקייה `.git`. |
| **Repository (repo)** | אלבום תמונות של הפרויקט לאורך הזמן. | התיקייה + כל ההיסטוריה שלה. |
| **Commit** | תמונה אחת באלבום, עם כיתוב "מה השתנה". | snapshot של הקבצים + הודעה + מחבר + מזהה ייחודי (hash). |
| **Staging (`git add`)** | לבחור אילו דברים ייכנסו לתמונה הבאה. | אזור ביניים לפני commit. |
| **Branch** | עולם מקביל: מנסים רעיון בלי לקלקל את הגרסה הטובה. | מצביע זז לשרשרת commits. `main` = הענף הראשי. |
| **Remote** | העותק של האלבום בכספת אצל חבר. | כתובת של repo אחר (בדרך כלל GitHub), בשם `origin`. |
| **Push** | לשלוח את התמונות החדשות לכספת. | העלאת commits מקומיים ל-remote. |
| **Pull** | להביא תמונות חדשות מהכספת. | `fetch` + `merge` מה-remote. |
| **GitHub** | כספת באינטרנט לאלבומים, עם אפשרות לשתף. | שירות אירוח ל-Git (חשבון חינמי כולל repos פרטיים ללא הגבלה). |
| **Private / Public** | אלבום נעול / אלבום בתערוכה. | מי יכול לראות את ה-repo. |
| **.gitignore** | רשימת "אסור לצלם" – סודות, זבל. | קבצים ש-Git מתעלם מהם. |

## 2. מה כבר נעשה בפרויקט

נבדק ב-sandbox שבו בניתי את הפרויקט (לא במחשב שלך):
- Git מותקן ✅, GitHub CLI (`gh`) מותקן ✅, אבל **אין אימות GitHub תקף** ב-sandbox (הטוקן לא תקף), ולכן לא יכולתי ליצור repo בעצמי.
- `git init` בוצע, ענף `main`.
- **commit אחרי כל שלב** (ראי `git log --oneline`):
  ```
  Phase 1: architecture, project structure and core domain model
  Phase 2: Kafka topics, producer, consumer-group auditor and event generator
  Phase 3: Spark Structured Streaming pipeline and batch report
  Phase 4: S3-compatible data lake (SeaweedFS) and PostgreSQL analytics store
  Phase 5: FastAPI analytics API, Streamlit dashboard and public demo mode
  Phase 6: Makefile, screenshots, demo snapshot and run/learning documentation
  ...
  ```
- `.gitignore` מונע: `.env`, מפתחות (`*.pem`, `*.key`, `*.crt`), `.aws/`, סביבות וירטואליות, `__pycache__`, נתונים שנוצרים (`data/`, `checkpoints/`, `*.parquet`, `*.log`).
- בדקתי שאין סודות בהיסטוריה: `git log --all -p | grep -i password` מחזיר רק את שמות המשתנים ואת הערך לדוגמה `change_me_local_only` מ-`.env.example`.

> ⚠️ הסיסמאות ב-`.env.example` הן **דוגמה**. ב-`.env` האמיתי שלך – שימי סיסמאות משלך. `.env` לעולם לא נכנס ל-Git.

## 3. יצירת repo פרטי ב-GitHub ודחיפה (הצעדים שלך)

### אפשרות א' – דרך האתר (הכי פשוט)
1. היכנסי ל-https://github.com/new
2. Repository name: **AdPulse**
3. בחרי **Private** 🔒
4. **אל** תסמני "Add a README" / ".gitignore" / "license" (כבר יש לנו – אחרת יהיה קונפליקט).
5. Create repository.

אחרי זה אני יכולה לחבר את ה-repo ולדחוף מכאן. או – מהמחשב שלך, מתוך תיקיית הפרויקט:
```bash
git remote add origin https://github.com/katya-alferov/AdPulse.git
git push -u origin main
```
(`-u` = "זכור ש-main שלי קשור ל-main ב-GitHub", כדי שבפעם הבאה יספיק `git push`.)

### אפשרות ב' – עם GitHub CLI מהמחשב שלך
```bash
gh auth login                          # פעם אחת: GitHub.com → HTTPS → Login with a web browser
gh repo create AdPulse --private --source=. --remote=origin --push
```

### אימות שהגיבוי הצליח
```bash
git remote -v                          # צריך להראות origin → github.com/katya-alferov/AdPulse
git status                             # "Your branch is up to date with 'origin/main'"
git log --oneline origin/main | head   # אותם commits כמו מקומית
gh repo view katya-alferov/AdPulse --json visibility -q .visibility   # PRIVATE
```
ובאתר: ודאי שרואים את `README.md`, `docker-compose.yml`, `src/`, `spark_jobs/` – ושאין `.env`.

## 4. עבודה יומיומית
```bash
git status                        # מה השתנה?
git diff                          # מה בדיוק השתנה?
git add src/adpulse/metrics.py    # בחרי קבצים (או: git add -A לכל השינויים)
git commit -m "Add ROAS to campaign summary"
git push
```
הודעת commit טובה: פועל בהווה + מה ולמה. ✅ `Fix CTR division by zero for new campaigns` ❌ `fix`, `changes`, `asdf`.

### לנסות רעיון בענף נפרד
```bash
git switch -c feature/sliding-window-fraud   # ענף חדש
# ...עבודה + commits...
git push -u origin feature/sliding-window-fraud
# ב-GitHub: Pull Request → Merge
git switch main && git pull
```

## 5. כללי בטיחות (קדושים!)
1. **לעולם לא** להכניס `.env`, סיסמאות, מפתחות AWS. לפני commit: `git status` – אם רואים `.env` → עצרי.
2. **לא** `git push --force` לענף משותף – זה מוחק היסטוריה אצל אחרים.
3. אם בטעות עשית commit לסוד: **לשנות את הסוד מיד** (להחליף סיסמה) – ורק אחר כך לנקות היסטוריה. סוד שנדחף ל-GitHub נחשב חשוף.
4. repo נשאר **פרטי** עד שתחליטי במפורש להפוך אותו לציבורי (ראי `PORTFOLIO_DEPLOYMENT_HE.md` – יש צ'קליסט לפני).

## 6. תקלות נפוצות
| שגיאה | פתרון |
|---|---|
| `remote origin already exists` | `git remote set-url origin https://github.com/katya-alferov/AdPulse.git` |
| `Authentication failed` / בקשת סיסמה | GitHub לא מקבל סיסמה רגילה – `gh auth login` או Personal Access Token |
| `rejected ... fetch first` | יש ב-GitHub commits שאין אצלך: `git pull --rebase` ואז `git push` |
| `src refspec main does not match any` | עוד אין commits, או שהענף נקרא `master`: `git branch -M main` |
