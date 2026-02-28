# 🚨 Quick Fix: Vercel "Failed to load dashboard data" Error

## The Problem
Your Vercel frontend can't find the backend because `VITE_API_BASE` environment variable is not set.

## The Solution (5 minutes)

### Step 1: Get Your Backend URL
1. Go to [Render Dashboard](https://dashboard.render.com/)
2. Click on your backend service
3. Copy the URL at the top (e.g., `https://duisburg-backend.onrender.com`)

### Step 2: Set Environment Variable in Vercel
1. Go to [Vercel Dashboard](https://vercel.com/dashboard)
2. Click on your project
3. Go to **Settings** → **Environment Variables**
4. Click **"Add New"**
5. Set:
   - **Name**: `VITE_API_BASE`
   - **Value**: Your Render URL (e.g., `https://duisburg-backend.onrender.com`)
   - **Environment**: Check **Production**, **Preview**, and **Development**
6. Click **"Save"**

### Step 3: Redeploy Frontend
1. Still in Vercel, go to **Deployments** tab
2. Click the **"..."** menu on the latest deployment
3. Click **"Redeploy"**
4. Check **"Use existing Build Cache"**
5. Click **"Redeploy"**

### Step 4: Configure CORS on Backend
1. Go back to Render Dashboard → Your backend service
2. Go to **Environment** (left sidebar)
3. Find or add `CORS_ORIGINS`
4. Set value to: `https://your-app.vercel.app` (use YOUR actual Vercel URL)
5. Click **"Save Changes"** (backend will auto-redeploy)

### Step 5: Test
Wait 2-3 minutes for both deployments to complete, then:
1. Open your Vercel URL
2. Dashboard should now load data!
3. Open browser console (F12) - should be no errors

---

## Still Not Working?

### Check These:
1. **VITE_API_BASE is correct**
   - Should NOT include `/api` at the end
   - Should be `https://`, not `http://`
   - Should be your Render URL, not Vercel URL

2. **Backend is running**
   - Visit: `https://your-backend.onrender.com/api/health`
   - Should return: `{"status":"OK",...}`

3. **CORS is configured**
   - Render → Environment → `CORS_ORIGINS` should have your Vercel URL
   - No typos in the URL

4. **Check Vercel Build Logs**
   - Vercel → Deployments → [latest] → Click to view logs
   - Look for build errors

5. **Check Browser Console**
   - F12 → Console tab
   - Look for red errors
   - Check Network tab → Failed requests

---

## Common Mistakes

❌ **Wrong**: `VITE_API_BASE=https://my-backend.onrender.com/api`
✅ **Correct**: `VITE_API_BASE=https://my-backend.onrender.com`

❌ **Wrong**: Setting in Render (backend)
✅ **Correct**: Setting in Vercel (frontend)

❌ **Wrong**: Not redeploying after adding variable
✅ **Correct**: Always redeploy after env var changes

---

**Read the full guide**: `DEPLOYMENT_GUIDE.md` for detailed instructions
