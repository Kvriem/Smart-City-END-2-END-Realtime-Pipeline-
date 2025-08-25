@echo off
echo Installing Smart City Dashboard dependencies...
echo.

cd dashboard

echo Installing Node.js dependencies...
npm install

echo.
echo Dependencies installed successfully!
echo.
echo To start the development server:
echo   cd dashboard
echo   npm run dev
echo.
echo The dashboard will be available at http://localhost:3000
echo Make sure the API is running on http://localhost:8000
