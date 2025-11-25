// ==============================================================================
// PURCHASE ORDER EXTRACTOR - Google Apps Script with Gemini AI
// Enhanced Version with Rate Limiting & Error Handling
// ==============================================================================
// This script processes purchase order PDFs, extracts product information,
// and writes each product as a separate row in Google Sheets.
// ==============================================================================

// --- CONFIGURATION ---
const CONFIG = {
  SOURCE_FOLDER_ID: '1h7H7fHYCK6Sl_tfmpH1DFAtVxSh1qWxO',      // Folder containing PO PDFs
  PROCESSED_FOLDER_ID: '1OB4YjEy2UvXpwaIboEPHK0MluweE9BnS',   // Folder for processed PDFs
  ERROR_FOLDER_ID: '1OB4YjEy2UvXpwaIboEPHK0MluweE9BnS',       // Folder for error files (can be same as processed)
  SHEET_ID: '1ix4DNDcUoROd2Gzl39JHKUCSVpNgPtUgef9ewIUzn84',
  SHEET_NAME: 'ExtractedData',
  AI_MODEL: 'gemini-2.0-flash-exp',
  PROJECT_ID: 'projectscannerai',
  LOCATION: 'us-central1',
  
  // Rate Limiting Configuration
  MAX_FILES_PER_RUN: 5,                    // Maximum PDFs to process per execution
  WAIT_BETWEEN_FILES: 12000,               // Wait time between files (12 seconds)
  MAX_RETRIES: 3,                          // Maximum retry attempts for 429 errors
  INITIAL_RETRY_DELAY: 60000,              // Initial retry delay (60 seconds)
  MAX_EXECUTION_TIME: 270000,              // Max execution time (4.5 minutes, leaving buffer)
  
  // API Rate Limits (adjust based on your quota)
  REQUESTS_PER_MINUTE: 15,                 // Conservative limit
  REQUESTS_PER_DAY: 1500
};

// --- SCRIPT PROPERTIES FOR STATE MANAGEMENT ---
const PROP_KEYS = {
  DAILY_REQUEST_COUNT: 'dailyRequestCount',
  LAST_RESET_DATE: 'lastResetDate',
  RATE_LIMIT_REQUESTS: 'rateLimitRequests',
  RATE_LIMIT_WINDOW_START: 'rateLimitWindowStart'
};

// ==============================================================================
// RATE LIMITER CLASS
// ==============================================================================

class RateLimiter {
  constructor() {
    this.scriptProperties = PropertiesService.getScriptProperties();
  }
  
  /**
   * Reset daily counter if it's a new day
   */
  resetDailyCounterIfNeeded() {
    const today = new Date().toDateString();
    const lastResetDate = this.scriptProperties.getProperty(PROP_KEYS.LAST_RESET_DATE);
    
    if (lastResetDate !== today) {
      this.scriptProperties.setProperty(PROP_KEYS.DAILY_REQUEST_COUNT, '0');
      this.scriptProperties.setProperty(PROP_KEYS.LAST_RESET_DATE, today);
      Logger.log('Daily request counter reset');
    }
  }
  
  /**
   * Check if we can make a request based on rate limits
   * @return {Object} {allowed: boolean, reason: string, waitTime: number}
   */
  canMakeRequest() {
    this.resetDailyCounterIfNeeded();
    
    // Check daily limit
    const dailyCount = parseInt(this.scriptProperties.getProperty(PROP_KEYS.DAILY_REQUEST_COUNT) || '0');
    if (dailyCount >= CONFIG.REQUESTS_PER_DAY) {
      return {
        allowed: false,
        reason: 'Daily API limit reached',
        waitTime: this.getTimeUntilMidnight()
      };
    }
    
    // Check per-minute limit
    const now = Date.now();
    const windowStart = parseInt(this.scriptProperties.getProperty(PROP_KEYS.RATE_LIMIT_WINDOW_START) || '0');
    const requestsInWindow = parseInt(this.scriptProperties.getProperty(PROP_KEYS.RATE_LIMIT_REQUESTS) || '0');
    
    const windowAge = now - windowStart;
    const oneMinute = 60000;
    
    // Reset window if older than 1 minute
    if (windowAge > oneMinute) {
      this.scriptProperties.setProperty(PROP_KEYS.RATE_LIMIT_WINDOW_START, now.toString());
      this.scriptProperties.setProperty(PROP_KEYS.RATE_LIMIT_REQUESTS, '0');
      return { allowed: true, reason: '', waitTime: 0 };
    }
    
    // Check if we've hit the per-minute limit
    if (requestsInWindow >= CONFIG.REQUESTS_PER_MINUTE) {
      const waitTime = oneMinute - windowAge + 1000; // Add 1 second buffer
      return {
        allowed: false,
        reason: 'Per-minute rate limit reached',
        waitTime: waitTime
      };
    }
    
    return { allowed: true, reason: '', waitTime: 0 };
  }
  
  /**
   * Record a successful API request
   */
  recordRequest() {
    this.resetDailyCounterIfNeeded();
    
    // Increment daily counter
    const dailyCount = parseInt(this.scriptProperties.getProperty(PROP_KEYS.DAILY_REQUEST_COUNT) || '0');
    this.scriptProperties.setProperty(PROP_KEYS.DAILY_REQUEST_COUNT, (dailyCount + 1).toString());
    
    // Increment window counter
    const requestsInWindow = parseInt(this.scriptProperties.getProperty(PROP_KEYS.RATE_LIMIT_REQUESTS) || '0');
    this.scriptProperties.setProperty(PROP_KEYS.RATE_LIMIT_REQUESTS, (requestsInWindow + 1).toString());
    
    Logger.log(`Request recorded. Daily: ${dailyCount + 1}/${CONFIG.REQUESTS_PER_DAY}, Window: ${requestsInWindow + 1}/${CONFIG.REQUESTS_PER_MINUTE}`);
  }
  
  /**
   * Wait if necessary before making a request
   * @return {boolean} true if request can proceed, false if should stop execution
   */
  waitIfNeeded() {
    const check = this.canMakeRequest();
    
    if (!check.allowed) {
      Logger.log(`⏸ ${check.reason}`);
      
      // If daily limit reached, we should stop the entire execution
      if (check.reason.includes('Daily')) {
        Logger.log('Stopping execution due to daily limit');
        return false;
      }
      
      // For per-minute limits, wait if we have time
      if (check.waitTime > 0 && check.waitTime < 55000) { // Less than 55 seconds
        Logger.log(`Waiting ${Math.ceil(check.waitTime/1000)} seconds for rate limit...`);
        Utilities.sleep(check.waitTime);
        return true;
      } else {
        Logger.log('Wait time too long, stopping execution');
        return false;
      }
    }
    
    return true;
  }
  
  /**
   * Get milliseconds until midnight
   */
  getTimeUntilMidnight() {
    const now = new Date();
    const tomorrow = new Date(now);
    tomorrow.setDate(tomorrow.getDate() + 1);
    tomorrow.setHours(0, 0, 0, 0);
    return tomorrow - now;
  }
  
  /**
   * Get current usage stats
   */
  getUsageStats() {
    this.resetDailyCounterIfNeeded();
    const dailyCount = parseInt(this.scriptProperties.getProperty(PROP_KEYS.DAILY_REQUEST_COUNT) || '0');
    const requestsInWindow = parseInt(this.scriptProperties.getProperty(PROP_KEYS.RATE_LIMIT_REQUESTS) || '0');
    
    return {
      dailyCount: dailyCount,
      dailyLimit: CONFIG.REQUESTS_PER_DAY,
      windowCount: requestsInWindow,
      windowLimit: CONFIG.REQUESTS_PER_MINUTE
    };
  }
}

// ==============================================================================
// MAIN PROCESSING FUNCTION
// ==============================================================================

/**
 * Main function to process new purchase orders from the source folder
 * This should be set up as a time-driven trigger (e.g., every 5-15 minutes)
 */
function processNewPurchaseOrders() {
  const startTime = Date.now();
  Logger.log('=== Starting Purchase Order Processing ===');
  
  const rateLimiter = new RateLimiter();
  const stats = rateLimiter.getUsageStats();
  Logger.log(`Current Usage - Daily: ${stats.dailyCount}/${stats.dailyLimit}, Window: ${stats.windowCount}/${stats.windowLimit}`);
  
  const sourceFolder = DriveApp.getFolderById(CONFIG.SOURCE_FOLDER_ID);
  const processedFolder = DriveApp.getFolderById(CONFIG.PROCESSED_FOLDER_ID);
  const errorFolder = DriveApp.getFolderById(CONFIG.ERROR_FOLDER_ID);
  const sheet = SpreadsheetApp.openById(CONFIG.SHEET_ID).getSheetByName(CONFIG.SHEET_NAME);
  
  // Initialize sheet headers if empty
  initializeSheetHeaders(sheet);
  
  const files = sourceFolder.getFilesByType(MimeType.PDF);
  let filesProcessedThisRun = 0;
  let filesSuccessful = 0;
  let filesFailed = 0;
  let filesRateLimited = 0;

  while (files.hasNext() && filesProcessedThisRun < CONFIG.MAX_FILES_PER_RUN) {
    // Check execution time limit
    const elapsedTime = Date.now() - startTime;
    if (elapsedTime > CONFIG.MAX_EXECUTION_TIME) {
      Logger.log(`⏱ Execution time limit reached (${Math.round(elapsedTime/1000)}s). Stopping.`);
      break;
    }
    
    const file = files.next();
    const fileName = file.getName();
    const fileUrl = file.getUrl();

    Logger.log(`\n--- Processing file: ${fileName} ---`);
    Logger.log(`File URL: ${fileUrl}`);

    try {
      // Check rate limits before processing
      if (!rateLimiter.waitIfNeeded()) {
        Logger.log('⏸ Rate limit reached, stopping batch processing');
        break;
      }
      
      // Extract data from PDF using Gemini AI with retry logic
      const extractedData = extractPurchaseOrderDataWithRetry(file, rateLimiter);
      
      if (extractedData.success) {
        // Write each product as a separate row
        writeProductsToSheet(sheet, extractedData, fileName, fileUrl);
        
        // Move file to processed folder
        file.moveTo(processedFolder);
        Logger.log(`✓ Successfully processed and moved ${fileName}`);
        filesSuccessful++;
      } else if (extractedData.rateLimited) {
        // Don't move file, will retry on next run
        Logger.log(`⏸ Rate limited on ${fileName}, will retry on next run`);
        filesRateLimited++;
        break; // Stop processing more files
      } else {
        // Log error and move to error folder
        logError(sheet, fileName, fileUrl, extractedData.error);
        Logger.log(`✗ Error processing ${fileName}: ${extractedData.error}`);
        
        // Move to error folder (or keep in processed folder)
        file.moveTo(errorFolder);
        filesFailed++;
      }
      
      filesProcessedThisRun++;
      
      // Wait before processing next file
      if (files.hasNext() && filesProcessedThisRun < CONFIG.MAX_FILES_PER_RUN) {
        Logger.log(`Waiting ${CONFIG.WAIT_BETWEEN_FILES/1000} seconds before next file...`);
        Utilities.sleep(CONFIG.WAIT_BETWEEN_FILES);
      }
      
    } catch (error) {
      Logger.log(`✗ Critical error processing ${fileName}: ${error.toString()}`);
      logError(sheet, fileName, fileUrl, error.toString());
      filesFailed++;
    }
  }

  const finalStats = rateLimiter.getUsageStats();
  Logger.log(`\n=== Batch Processing Complete ===`);
  Logger.log(`Files Processed: ${filesProcessedThisRun}`);
  Logger.log(`├─ Successful: ${filesSuccessful}`);
  Logger.log(`├─ Failed: ${filesFailed}`);
  Logger.log(`└─ Rate Limited: ${filesRateLimited}`);
  Logger.log(`API Usage - Daily: ${finalStats.dailyCount}/${finalStats.dailyLimit}`);
  Logger.log(`Execution Time: ${Math.round((Date.now() - startTime)/1000)}s`);
}

// ==============================================================================
// GEMINI AI EXTRACTION WITH RETRY LOGIC
// ==============================================================================

/**
 * Extracts purchase order data with exponential backoff retry logic
 * @param {File} file - Google Drive file object
 * @param {RateLimiter} rateLimiter - Rate limiter instance
 * @return {Object} Extraction result
 */
function extractPurchaseOrderDataWithRetry(file, rateLimiter) {
  let lastError = null;
  
  for (let attempt = 0; attempt < CONFIG.MAX_RETRIES; attempt++) {
    if (attempt > 0) {
      const delay = CONFIG.INITIAL_RETRY_DELAY * Math.pow(2, attempt - 1); // Exponential backoff
      Logger.log(`Retry attempt ${attempt}/${CONFIG.MAX_RETRIES - 1} after ${Math.ceil(delay/1000)}s delay...`);
      Utilities.sleep(delay);
      
      // Check if we can retry based on rate limits
      if (!rateLimiter.waitIfNeeded()) {
        return {
          success: false,
          rateLimited: true,
          error: 'Rate limit reached during retry'
        };
      }
    }
    
    const result = extractPurchaseOrderData(file, rateLimiter);
    
    if (result.success) {
      return result;
    }
    
    // If rate limited (429), mark for retry
    if (result.rateLimited) {
      lastError = result;
      Logger.log(`Rate limit hit (429), will retry...`);
      continue;
    }
    
    // For non-retryable errors, return immediately
    if (!result.retryable) {
      return result;
    }
    
    lastError = result;
  }
  
  // All retries exhausted
  return lastError || {
    success: false,
    error: 'All retry attempts exhausted'
  };
}

/**
 * Extracts purchase order data from a PDF file using Gemini AI
 * @param {File} file - Google Drive file object
 * @param {RateLimiter} rateLimiter - Rate limiter instance
 * @return {Object} Extraction result with success flag and data/error
 */
function extractPurchaseOrderData(file, rateLimiter) {
  try {
    const fileBlob = file.getBlob();
    const base64Data = Utilities.base64Encode(fileBlob.getBytes());

    const prompt = `
You are a purchase order data extraction expert. Extract ALL products from this purchase order PDF.

IMPORTANT: Identify the BASE/LOCATION from the shipping address:
- If address contains "MIAMI" or "FL" → Base: "Miami"
- If address contains "CALIFORNIA" or "CA" → Base: "California"
- If address contains "PORTUGAL" or "PT" → Base: "Portugal"
- If address contains "PRAGUE" or "PRAHA" → Base: "Prague"
- If address contains "BRNO" → Base: "Brno"
- If unclear → Base: "Unknown"

Return ONLY a valid JSON object with this exact structure:
{
  "orderNumber": "17057666",
  "orderDate": "2025-01-07",
  "source": "Aircraft Spruce",
  "base": "Miami",
  "shippingAddress": "SILVER EXPRESS 14569 SOUTHWEST 127TH ST MIAMI, FL 33186",
  "products": [
    {
      "productName": "WINDOW TYPE BUTT SPLICE YELLOW",
      "partNumber": "320570",
      "quantity": 15.00,
      "unitPrice": 1.20,
      "totalPrice": 15.30
    }
  ]
}

CRITICAL RULES:
1. Extract ALL products - there may be multiple products in one order
2. Identify the base from the shipping address
3. Include the full shipping address
4. Each product must include: productName, partNumber, quantity, unitPrice, totalPrice
5. Use "N/A" for missing text fields, 0 for missing numeric fields
6. Dates should be in YYYY-MM-DD format
7. Numbers should be numeric values (not strings)
8. Do NOT include markdown formatting or code fences
9. Return ONLY the JSON object, nothing else

Extract the following from the purchase order:
- Order number/ID
- Order date
- Supplier/source name
- Shipping address (full address)
- Base/location (determined from shipping address)
- For each product:
  * Product name/description
  * Part number/SKU
  * Quantity ordered
  * Unit price
  * Total price for that product
`;

    const requestBody = {
      contents: [
        {
          role: "user",
          parts: [
            { text: prompt },
            {
              inlineData: {
                mimeType: MimeType.PDF,
                data: base64Data
              }
            }
          ]
        }
      ],
      generationConfig: {
        temperature: 0.1,
        maxOutputTokens: 8192
      }
    };

    // Vertex AI endpoint
    const url = `https://${CONFIG.LOCATION}-aiplatform.googleapis.com/v1/projects/${CONFIG.PROJECT_ID}/locations/${CONFIG.LOCATION}/publishers/google/models/${CONFIG.AI_MODEL}:generateContent`;

    const options = {
      method: 'post',
      contentType: 'application/json',
      headers: {
        Authorization: 'Bearer ' + ScriptApp.getOAuthToken()
      },
      payload: JSON.stringify(requestBody),
      muteHttpExceptions: true
    };

    const response = UrlFetchApp.fetch(url, options);
    const responseCode = response.getResponseCode();
    const responseText = response.getContentText();

    Logger.log(`Gemini API Response Code: ${responseCode}`);
    
    // Record successful request (non-error codes)
    if (responseCode === 200) {
      rateLimiter.recordRequest();
    }

    if (responseCode === 200) {
      const jsonResponse = JSON.parse(responseText);
      const candidate = jsonResponse.candidates && jsonResponse.candidates[0];

      if (candidate && candidate.content && candidate.content.parts && candidate.content.parts[0]) {
        let textOutput = candidate.content.parts[0].text || "";
        
        // Clean up response - remove markdown code fences if present
        textOutput = textOutput.replace(/^```json\s*|```\s*$/g, '').trim();
        
        Logger.log(`AI Cleaned Output: ${textOutput.substring(0, 500)}...`);

        try {
          const extractedData = JSON.parse(textOutput);
          
          // Validate the structure
          if (!extractedData.products || !Array.isArray(extractedData.products)) {
            return {
              success: false,
              retryable: false,
              error: "Invalid JSON structure: missing or invalid products array"
            };
          }
          
          return {
            success: true,
            data: extractedData
          };
          
        } catch (parseError) {
          Logger.log(`JSON Parse Error: ${parseError.message}`);
          Logger.log(`Raw AI Output: ${textOutput}`);
          return {
            success: false,
            retryable: false,
            error: `JSON parsing failed: ${parseError.message}`,
            rawResponse: textOutput
          };
        }
      } else {
        return {
          success: false,
          retryable: false,
          error: "Unexpected API response structure",
          rawResponse: responseText
        };
      }
    } else if (responseCode === 429) {
      // Rate limit hit - check for Retry-After header
      const retryAfter = response.getHeaders()['Retry-After'];
      const retryDelay = retryAfter ? parseInt(retryAfter) * 1000 : CONFIG.INITIAL_RETRY_DELAY;
      
      Logger.log(`Rate limit (429) hit. Retry after: ${retryDelay/1000}s`);
      
      return {
        success: false,
        rateLimited: true,
        retryable: true,
        retryAfter: retryDelay,
        error: `API Rate Limit (${responseCode})`,
        rawResponse: responseText
      };
    } else if (responseCode === 503 || responseCode === 500) {
      // Server errors - retryable
      Logger.log(`Server error (${responseCode}), retryable`);
      return {
        success: false,
        retryable: true,
        error: `API Server Error (${responseCode})`,
        rawResponse: responseText
      };
    } else {
      // Other errors - not retryable
      return {
        success: false,
        retryable: false,
        error: `API Call Failed (${responseCode})`,
        rawResponse: responseText
      };
    }

  } catch (error) {
    Logger.log(`Exception in extractPurchaseOrderData: ${error.toString()}`);
    return {
      success: false,
      retryable: false,
      error: error.toString()
    };
  }
}

// ==============================================================================
// SHEET WRITING FUNCTIONS
// ==============================================================================

function initializeSheetHeaders(sheet) {
  if (sheet.getLastRow() === 0) {
    const headers = [
      'Timestamp',
      'File Name',
      'File Link',
      'Order Number',
      'Order Date',
      'Source/Supplier',
      'Base',
      'Shipping Address',
      'Product Name',
      'Part Number',
      'Quantity',
      'Unit Price',
      'Total Price',
      'Status',
      'Notes'
    ];
    sheet.appendRow(headers);
    
    // Format header row
    const headerRange = sheet.getRange(1, 1, 1, headers.length);
    headerRange.setFontWeight('bold');
    headerRange.setBackground('#4285f4');
    headerRange.setFontColor('#ffffff');
    
    Logger.log('Sheet headers initialized');
  }
}

/**
 * Write extracted products to Google Sheet (one row per product)
 */
function writeProductsToSheet(sheet, extractedData, fileName, fileUrl) {
  const timestamp = new Date();
  const data = extractedData.data;
  
  const orderNumber = data.orderNumber || 'N/A';
  const orderDate = data.orderDate || 'N/A';
  const source = data.source || 'N/A';
  const base = data.base || 'Unknown';
  const shippingAddress = data.shippingAddress || 'N/A';
  
  // Write each product as a separate row
  if (data.products && data.products.length > 0) {
    data.products.forEach((product, index) => {
      const row = [
        timestamp,
        fileName,
        fileUrl,
        orderNumber,
        orderDate,
        source,
        base,
        shippingAddress,
        product.productName || 'N/A',
        product.partNumber || 'N/A',
        product.quantity || 0,
        product.unitPrice || 0,
        product.totalPrice || 0,
        'Success',
        `Product ${index + 1} of ${data.products.length}`
      ];
      
      sheet.appendRow(row);
    });
    
    Logger.log(`✓ Wrote ${data.products.length} products to sheet for base: ${base}`);
  } else {
    // No products found
    const row = [
      timestamp,
      fileName,
      fileUrl,
      orderNumber,
      orderDate,
      source,
      base,
      shippingAddress,
      'N/A',
      'N/A',
      0,
      0,
      0,
      'Warning',
      'No products extracted'
    ];
    sheet.appendRow(row);
    Logger.log('⚠ No products found in extraction');
  }
}

/**
 * Log an error to the sheet
 */
function logError(sheet, fileName, fileUrl, errorMessage) {
  const row = [
    new Date(),
    fileName,
    fileUrl,
    'N/A',
    'N/A',
    'N/A',
    'Unknown',
    'N/A',
    'N/A',
    'N/A',
    0,
    0,
    0,
    'Error',
    errorMessage.substring(0, 1000)
  ];
  
  sheet.appendRow(row);
}

// ==============================================================================
// UTILITY & TESTING FUNCTIONS
// ==============================================================================

/**
 * Reset the sheet with correct headers
 */
function resetSheetHeaders() {
  const sheet = SpreadsheetApp.openById(CONFIG.SHEET_ID).getSheetByName(CONFIG.SHEET_NAME);
  
  sheet.deleteRow(1);
  
  const headers = [
    'Timestamp',
    'File Name',
    'File Link',
    'Order Number',
    'Order Date',
    'Source/Supplier',
    'Base',
    'Shipping Address',
    'Product Name',
    'Part Number',
    'Quantity',
    'Unit Price',
    'Total Price',
    'Status',
    'Notes'
  ];
  
  sheet.insertRowBefore(1);
  sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  
  const headerRange = sheet.getRange(1, 1, 1, headers.length);
  headerRange.setFontWeight('bold');
  headerRange.setBackground('#4285f4');
  headerRange.setFontColor('#ffffff');
  
  Logger.log('✓ Sheet headers reset successfully!');
}

/**
 * View current rate limiter statistics
 */
function viewRateLimiterStats() {
  const rateLimiter = new RateLimiter();
  const stats = rateLimiter.getUsageStats();
  
  Logger.log('=== Rate Limiter Statistics ===');
  Logger.log(`Daily Requests: ${stats.dailyCount}/${stats.dailyLimit} (${Math.round(stats.dailyCount/stats.dailyLimit*100)}%)`);
  Logger.log(`Current Window: ${stats.windowCount}/${stats.windowLimit}`);
  
  const remaining = stats.dailyLimit - stats.dailyCount;
  Logger.log(`Remaining Today: ${remaining} requests`);
  
  if (remaining < 100) {
    Logger.log('⚠️ WARNING: Approaching daily limit!');
  }
}

/**
 * Reset rate limiter counters (use carefully!)
 */
function resetRateLimiter() {
  const scriptProperties = PropertiesService.getScriptProperties();
  scriptProperties.deleteProperty(PROP_KEYS.DAILY_REQUEST_COUNT);
  scriptProperties.deleteProperty(PROP_KEYS.LAST_RESET_DATE);
  scriptProperties.deleteProperty(PROP_KEYS.RATE_LIMIT_REQUESTS);
  scriptProperties.deleteProperty(PROP_KEYS.RATE_LIMIT_WINDOW_START);
  
  Logger.log('✓ Rate limiter reset complete');
}

/**
 * Test function to process a single file
 */
function testProcessSingleFile() {
  Logger.log('=== TESTING SINGLE FILE PROCESSING ===');
  processNewPurchaseOrders();
}

/**
 * Manually process a specific file by name (for testing)
 */
function testSpecificFile(targetFileName) {
  Logger.log(`=== Testing specific file: ${targetFileName} ===`);
  
  const rateLimiter = new RateLimiter();
  const sourceFolder = DriveApp.getFolderById(CONFIG.SOURCE_FOLDER_ID);
  const sheet = SpreadsheetApp.openById(CONFIG.SHEET_ID).getSheetByName(CONFIG.SHEET_NAME);
  
  initializeSheetHeaders(sheet);
  
  const files = sourceFolder.getFilesByType(MimeType.PDF);
  
  while (files.hasNext()) {
    const file = files.next();
    const fileName = file.getName();
    
    if (fileName === targetFileName || fileName.includes(targetFileName)) {
      Logger.log(`Found file: ${fileName}`);
      const fileUrl = file.getUrl();
      
      try {
        if (!rateLimiter.waitIfNeeded()) {
          Logger.log('⏸ Rate limit reached, cannot process file');
          return;
        }
        
        const extractedData = extractPurchaseOrderDataWithRetry(file, rateLimiter);
        
        if (extractedData.success) {
          writeProductsToSheet(sheet, extractedData, fileName, fileUrl);
          Logger.log('✓ Test successful!');
          Logger.log(`Extracted data: ${JSON.stringify(extractedData.data, null, 2)}`);
        } else {
          logError(sheet, fileName, fileUrl, extractedData.error);
          Logger.log(`✗ Test failed: ${extractedData.error}`);
        }
      } catch (error) {
        Logger.log(`✗ Test error: ${error.toString()}`);
        logError(sheet, fileName, fileUrl, error.toString());
      }
      
      return;
    }
  }
  
  Logger.log(`File not found: ${targetFileName}`);
}

/**
 * Setup function - creates sheet and folders if needed
 */
function setupEnvironment() {
  Logger.log('=== Setting up environment ===');
  
  try {
    // Check for source folder
    try {
      DriveApp.getFolderById(CONFIG.SOURCE_FOLDER_ID);
      Logger.log('✓ Source folder exists');
    } catch (e) {
      Logger.log('⚠ Source folder not found. Please create it and update CONFIG.SOURCE_FOLDER_ID');
    }
    
    // Check for processed folder
    try {
      DriveApp.getFolderById(CONFIG.PROCESSED_FOLDER_ID);
      Logger.log('✓ Processed folder exists');
    } catch (e) {
      Logger.log('⚠ Processed folder not found. Please create it and update CONFIG.PROCESSED_FOLDER_ID');
    }
    
    // Check for spreadsheet
    try {
      const sheet = SpreadsheetApp.openById(CONFIG.SHEET_ID).getSheetByName(CONFIG.SHEET_NAME);
      initializeSheetHeaders(sheet);
      Logger.log('✓ Spreadsheet found and headers initialized');
    } catch (e) {
      Logger.log('⚠ Spreadsheet not found. Please create it and update CONFIG.SHEET_ID');
    }
    
    // Initialize rate limiter
    const rateLimiter = new RateLimiter();
    rateLimiter.resetDailyCounterIfNeeded();
    Logger.log('✓ Rate limiter initialized');
    
    Logger.log('\n=== Setup complete! ===');
    Logger.log('Next steps:');
    Logger.log('1. Upload a test PDF to your source folder');
    Logger.log('2. Run testProcessSingleFile() to test');
    Logger.log('3. Run viewRateLimiterStats() to check API usage');
    Logger.log('4. Set up a time-driven trigger for processNewPurchaseOrders()');
    Logger.log('   Recommended: Every 10-15 minutes');
    
  } catch (error) {
    Logger.log(`Setup error: ${error.toString()}`);
  }
}
