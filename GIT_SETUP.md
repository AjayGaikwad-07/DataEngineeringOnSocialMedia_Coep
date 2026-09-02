# Pushing SocialMedia_DE_Practicals to GitHub

Run these from inside the `SocialMedia_DE_Practicals` folder in VS
Code's terminal. Replace `<your-username>` and `<repo-name>` where
shown.

## 1. Drop in the two new files
Save `.gitignore` and `README.md` (from this chat) into the project
root, replacing the existing README.md.

## 2. Initialize git (skip if already done)

```powershell
git init
git branch -M main
```

## 3. Check what will be committed

```powershell
git status
```

Make sure `data/raw/*.json`, `data/landing/*.jsonl`, and
`dashboard/output/*.png` do NOT show up as untracked-to-be-added —
the `.gitignore` should already exclude them. If Docker or Python
already created a `.venv` folder, confirm that's excluded too.

## 4. First commit

```powershell
git add .
git commit -m "Initial commit: social media DE pipeline (Kafka, Spark, Postgres, Airflow, Docker)"
```

## 5. Create the GitHub repo

**Option A — GitHub CLI** (if you have `gh` installed and are logged in):
```powershell
gh repo create SocialMedia_DE_Practicals --public --source=. --remote=origin
git push -u origin main
```

**Option B — no CLI:**
1. Go to https://github.com/new
2. Repo name: `SocialMedia_DE_Practicals` (or whatever you prefer)
3. Leave "Initialize with README" unchecked (you already have one)
4. Click **Create repository**
5. Copy the commands GitHub shows under "…or push an existing
   repository from the command line" — they'll look like:

```powershell
git remote add origin https://github.com/<your-username>/<repo-name>.git
git push -u origin main
```

## 6. Verify

Refresh the GitHub repo page — you should see all the folders
(`producer/`, `consumer/`, `spark/`, `dashboard/`, `airflow/`, etc.)
with the sample data and generated PNGs excluded, and the README
rendering on the front page.

## 7. Add screenshots to the README (optional, recommended for submission)

Once you've run the pipeline for real and have screenshots:
1. Create a folder `docs/screenshots/` in the repo
2. Drop your PNGs there
3. In `README.md`, replace the "Sample output" section with actual
   `![description](docs/screenshots/filename.png)` lines
4. `git add docs/screenshots README.md && git commit -m "Add output screenshots" && git push`

That gives you a repo link you can hand to your professor directly,
with real evidence embedded in the README itself.
