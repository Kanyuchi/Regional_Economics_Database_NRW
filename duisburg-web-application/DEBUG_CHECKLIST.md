# 🔍 Debugging Checklist - Vercel Deployment Error

Follow these steps in order to diagnose the issue:

## Step 1: Test Backend Health ✅

1. **Get your Render backend URL**
   - Go to: https://dashboard.render.com/
   - Click on your backend service
   - Copy the URL at the top (example: `https://duisburg-backend-abc123.onrender.com`)

2. **Test the health endpoint**
   - In your browser, visit: `https://YOUR-RENDER-URL/api/health`
   - Replace `YOUR-RENDER-URL` with your actual URL
   - **Expected result**:
     ```json
     {"status":"OK","message":"Duisburg Dashboard API is running"}
     ```
   - **If you see**: "Cannot GET /api/health" or error → Backend deployment issue (see Step 5)
   - **If you see**: JSON with status OK → Backend is working! ✅ Continue to Step 2

---

## Step 2: Find Your Vercel App URL 🔗

1. **Go to**: https://vercel.com/dashboard
2. **Click** on your project (e.g., "duisburg-dashboard")
3. **Look for the "Domains" section** - you'll see your URL like:
   - `https://duisburg-dashboard.vercel.app`
   - `https://duisburg-dashboard-xyz123.vercel.app`
4. **Copy this URL** - this is your deployed app, NOT the settings page!

---

## Step 3: Check Vercel Environment Variables 🔧

1. Still in your Vercel project, go to: **Settings** → **Environment Variables**
2. Look for `VITE_API_BASE`
3. **Check the value**:
   - ✅ Should be: `https://your-backend.onrender.com` (NO `/api` at end)
   - ❌ Wrong: `https://your-backend.onrender.com/api`
   - ❌ Wrong: `http://...` (must be `https`)
   - ❌ Wrong: `localhost:3001`

4. **Check the environment**:
   - Should be checked for: Production ✅
   - Optionally also: Preview and Development

5. **If you changed it**: You MUST redeploy!
   - Go to **Deployments** tab
   - Click "..." on latest deployment
   - Click **Redeploy**

---

## Step 4: Check Browser Console on Your App 💻

1. **Open your deployed app URL** (from Step 2)
   - Example: `https://duisburg-dashboard.vercel.app`

2. **Open Developer Tools**:
   - **Chrome/Edge**: Press `F12` or `Ctrl+Shift+I` (Windows) / `Cmd+Option+I` (Mac)
   - **Firefox**: Press `F12`
   - **Safari**: Enable Developer menu first (Safari → Preferences → Advanced → Show Develop menu)

3. **Click the "Console" tab** (if not already selected)

4. **Look for red error messages**. Common errors:

   ### Error A: CORS Error
   ```
   Access to fetch at 'https://your-backend.onrender.com/api/cities' from origin
   'https://your-app.vercel.app' has been blocked by CORS policy
   ```
   **Fix**: See Step 6 (CORS Configuration)

   ### Error B: Network Error / Failed to fetch
   ```
   Failed to load resource: net::ERR_NAME_NOT_RESOLVED
   ```
   **Fix**: Check VITE_API_BASE is set correctly (see Step 3)

   ### Error C: 404 Not Found
   ```
   GET https://your-app.vercel.app/api/cities 404 (Not Found)
   ```
   **Fix**: VITE_API_BASE not set! It's calling Vercel instead of Render. Go to Step 3.

   ### Error D: 503 Service Unavailable
   ```
   GET https://your-backend.onrender.com/api/cities 503
   ```
   **Fix**: Backend crashed or database connection failed. See Step 5.

5. **Also check the "Network" tab**:
   - Click **Network** tab
   - Refresh the page (`Ctrl+R` or `Cmd+R`)
   - Look for `/api/cities` or `/api/indicators` requests
   - Click on them to see:
     - **Request URL**: Should point to your Render backend
     - **Status Code**: Should be 200 (green)
     - **Response**: Should show JSON data

---

## Step 5: Check Render Backend Logs 📋

If backend health check failed in Step 1:

1. **Go to**: https://dashboard.render.com/
2. **Click** on your backend service
3. **Click** "Logs" in the left sidebar
4. **Look for errors** (red text):

   ### Common Error A: Database Connection Failed
   ```
   Database connection error: ECONNREFUSED
   or
   SSL SYSCALL error
   ```
   **Fix**: Check database environment variables:
   - `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` all set?
   - `DB_SSL=true` or `PGSSLMODE=require` set?
   - Database is running?

   ### Common Error B: Missing Environment Variable
   ```
   Environment variable DB_HOST is not defined
   ```
   **Fix**: Go to Render → Environment → Add the missing variable

   ### Common Error C: Port Binding Error
   ```
   Error: listen EADDRINUSE: address already in use :::3001
   ```
   **Fix**: This shouldn't happen on Render. Check your `server.js` uses `process.env.PORT`

5. **Check the "Events" tab**:
   - Shows deployment history
   - If deploy failed, click on it to see build logs

---

## Step 6: Configure CORS (If you see CORS error) 🔐

If Step 4 showed a CORS error:

1. **Go to**: https://dashboard.render.com/ → Your backend service
2. **Click**: Environment (left sidebar)
3. **Find or Add**: `CORS_ORIGINS`
4. **Set value to**: Your Vercel app URL (from Step 2)
   - Example: `https://duisburg-dashboard.vercel.app`
   - ⚠️ Must be EXACTLY the same as your Vercel domain
   - ⚠️ NO trailing slash
   - ⚠️ Must be `https`, not `http`

5. **Click** "Save Changes"
6. **Wait** 30-60 seconds for backend to redeploy
7. **Test** your Vercel app again

**Multiple domains?** Separate with commas:
```
https://duisburg-dashboard.vercel.app,https://yourdomain.com
```

---

## Step 7: Verify the Fix ✅

After making changes:

1. **Wait 2-3 minutes** for deployments to complete
2. **Clear browser cache**: `Ctrl+Shift+Del` or `Cmd+Shift+Del` → Clear cache
3. **Hard refresh**: `Ctrl+F5` or `Cmd+Shift+R`
4. **Open your Vercel app URL**
5. **Check**:
   - ✅ Dashboard loads with data
   - ✅ No errors in browser console
   - ✅ Can see cities and indicators

---

## Quick Reference Table

| Symptom | Likely Cause | Fix |
|---------|--------------|-----|
| "Cannot GET /" when testing root URL | Normal behavior | Test `/api/health` instead |
| "Cannot GET /api/health" | Route not registered or wrong path | Check `server.js` has the route |
| CORS error in browser console | CORS_ORIGINS not set | Step 6 |
| API calls go to Vercel instead of Render | VITE_API_BASE not set | Step 3 |
| 503 Service Unavailable | Backend crashed | Step 5 - check logs |
| 404 on all API calls | Wrong backend URL | Step 3 - check URL |
| Still showing port 3001 error | Old build cached | Step 3 - redeploy Vercel |

---

## Still Not Working? 🆘

**Collect this information and share it:**

1. **Render backend URL**: `https://your-backend.onrender.com`
2. **Result of health check**: `https://your-backend.onrender.com/api/health`
3. **Vercel app URL**: `https://your-app.vercel.app`
4. **VITE_API_BASE value** (from Vercel settings)
5. **CORS_ORIGINS value** (from Render settings)
6. **Browser console errors** (take a screenshot or copy the text)
7. **Render logs** (last 20 lines from the Logs tab)

With this info, we can pinpoint the exact issue!

---

## Pro Tips 💡

- **Free tier Render**: Backend spins down after 15 min idle. First request takes 30+ sec.
- **Environment variables**: Always redeploy after changing them!
- **Browser cache**: Always hard refresh (`Ctrl+F5`) when testing changes
- **Multiple tabs**: Close old tabs of your app to avoid cache confusion
- **Incognito mode**: Test in incognito to rule out cache issues

---

**Good luck!** 🚀 Follow each step carefully and the issue will reveal itself.
