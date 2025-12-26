import express from 'express';
import multer from 'multer';
import archiver from 'archiver';
import { PDFDocument, rgb } from 'pdf-lib';
import dotenv from 'dotenv';
import fetch from 'node-fetch';
import fs from 'fs';
import fontkit from '@pdf-lib/fontkit';
import cors from 'cors';
import bcrypt from 'bcrypt';
import { createClient } from '@supabase/supabase-js';
import { fileURLToPath } from 'url';
import path from 'path';
import crypto from 'crypto';

dotenv.config();
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ------------------------
// Supabase Client Setup
// ------------------------
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.error('❌ Missing Supabase credentials in .env file');
  console.error('Add: SUPABASE_URL=your-project-url');
  console.error('Add: SUPABASE_SERVICE_ROLE_KEY=your-service-role-key');
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

// ------------------------
// Express App
// ------------------------
const app = express();
const port = process.env.PORT || 5000;

app.use(cors({
  origin: '*',
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true }));

const upload = multer({ storage: multer.memoryStorage() });

// ------------------------
// Helper Functions
// ------------------------
async function getBufferFromUrl(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error('Failed to fetch image');
  return Buffer.from(await res.arrayBuffer());
}

// Helper to verify user ownership
async function verifyUserOwnership(userId, jobId) {
  try {
    const { data: jobData, error } = await supabase
      .from('jobs')
      .select('user_id')
      .eq('id', jobId)
      .single();

    if (error) return { valid: false, error: 'Job not found' };
    if (jobData.user_id !== userId) return { valid: false, error: 'Unauthorized' };
    
    return { valid: true, jobData };
  } catch (err) {
    return { valid: false, error: err.message };
  }
}

// ------------------------
// Auth Endpoints
// ------------------------
app.post('/signup', async (req, res) => {
  const { email, password } = req.body;
  
  try {
    const userId = crypto.randomUUID();
    
    const { data: userData, error: userError } = await supabase
      .from('users')
      .insert([{
        id: userId,
        email,
        password_hash: await bcrypt.hash(password, 10)
      }])
      .select()
      .single();

    if (userError) {
      if (userError.code === '23505') {
        return res.status(400).json({ error: 'Email already exists' });
      }
      throw userError;
    }

    res.json({ 
      success: true, 
      user: { 
        id: userId,
        email 
      } 
    });
  } catch (err) {
    console.error('Signup error:', err);
    res.status(500).json({ error: 'Signup failed: ' + err.message });
  }
});

app.post('/login', async (req, res) => {
  const { email, password } = req.body;
  
  try {
    const { data: userData, error } = await supabase
      .from('users')
      .select('*')
      .eq('email', email)
      .single();

    if (error || !userData) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    const match = await bcrypt.compare(password, userData.password_hash);
    if (!match) return res.status(401).json({ error: 'Invalid credentials' });

    res.json({ 
      success: true, 
      user: { 
        id: userData.id,
        email: userData.email 
      } 
    });
  } catch (err) {
    console.error('Login error:', err);
    res.status(500).json({ error: 'Login failed: ' + err.message });
  }
});

// ------------------------
// Upload Template to Supabase Storage
// ------------------------
app.post('/upload-template', upload.single('template'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No file' });

  try {
    const fileName = `templates/${Date.now()}_${req.file.originalname}`;
    
    const { data, error } = await supabase.storage
      .from('certificates')
      .upload(fileName, req.file.buffer, {
        contentType: req.file.mimetype,
        cacheControl: '3600',
      });

    if (error) throw error;

    const { data: urlData } = supabase.storage
      .from('certificates')
      .getPublicUrl(fileName);

    res.json({ 
      templateUrl: urlData.publicUrl,
      storagePath: fileName,
      message: 'Template uploaded successfully'
    });
  } catch (err) {
    console.error('Template upload error:', err);
    res.status(500).json({ error: 'Upload failed: ' + err.message });
  }
});

// ------------------------
// Upload CSV to Supabase Storage
// ------------------------
app.post('/upload-csv', upload.single('csv'), async (req, res) => {
  if (!req.file) return res.status(400).json({ error: 'No CSV' });

  try {
    const fileName = `csv/${Date.now()}_${req.file.originalname}`;
    
    const { data, error } = await supabase.storage
      .from('certificates')
      .upload(fileName, req.file.buffer, {
        contentType: 'text/csv',
        cacheControl: '3600',
      });

    if (error) throw error;

    const { data: urlData } = supabase.storage
      .from('certificates')
      .getPublicUrl(fileName);

    const csvContent = req.file.buffer.toString('utf8');
    
    res.json({ 
      csvStoragePath: fileName,
      csvUrl: urlData.publicUrl,
      csvContent: csvContent,
      rowCount: csvContent.split('\n').length - 1,
      message: 'CSV uploaded successfully'
    });
  } catch (err) {
    console.error('CSV upload error:', err);
    res.status(500).json({ error: 'Failed to upload CSV' });
  }
});

// ------------------------
// Generate PDFs - Direct ZIP Download (No ZIP Storage)
// ------------------------
// Add near the top of server.js after imports
import { EventEmitter } from 'events';
const jobEvents = new EventEmitter();

// ------------------------
// SSE Progress Streaming Endpoint
// ------------------------
app.get('/progress/:jobId', async (req, res) => {
  const jobId = req.params.jobId;
  
  // Set headers for SSE
  res.setHeader('Content-Type', 'text/event-stream');
  res.setHeader('Cache-Control', 'no-cache');
  res.setHeader('Connection', 'keep-alive');
  res.setHeader('Access-Control-Allow-Origin', '*');
  
  // Send initial connection
  res.write(`data: ${JSON.stringify({ type: 'connected', jobId })}\n\n`);
  
  // Listen for progress updates
  const progressHandler = (data) => {
    if (data.jobId === jobId) {
      res.write(`data: ${JSON.stringify(data)}\n\n`);
      
      // Flush the response
      if (typeof res.flush === 'function') {
        res.flush();
      }
    }
  };
  
  jobEvents.on('progress', progressHandler);
  
  // Handle client disconnect
  req.on('close', () => {
    jobEvents.off('progress', progressHandler);
    console.log(`SSE connection closed for job ${jobId}`);
    res.end();
  });
});

// ------------------------
// Generate PDFs with REAL-TIME PROGRESS
// ------------------------
app.post('/generate', async (req, res) => {
  const { userId, csvStoragePath, templateUrl, fields } = req.body;
  
  if (!userId || !csvStoragePath) {
    return res.status(400).json({ error: 'Missing required parameters' });
  }

  // Create job ID early for SSE
  const jobId = crypto.randomUUID();
  
  // Send job ID immediately for frontend to connect
  res.json({ 
    jobId,
    message: 'Generation started. Connect to /progress/:jobId for updates.',
    progressUrl: `/progress/${jobId}`
  });
  
  // Process in background
  processGenerationInBackground(jobId, userId, csvStoragePath, templateUrl, fields);
});

// ------------------------
// Background Processing Function
// ------------------------
// ------------------------
// Enhanced Background Processing Function with Concurrent Generation
// ------------------------
async function processGenerationInBackground(jobId, userId, csvStoragePath, templateUrl, fields) {
  let participants = [];
  let workerPool = null;
  
  try {
    // Emit initial progress
    jobEvents.emit('progress', {
      jobId,
      type: 'started',
      percent: 0,
      message: 'Starting certificate generation...',
      stage: 'initializing',
      processed: 0,
      total: 0,
      timestamp: new Date().toISOString()
    });

    // 1. Create job record
    await supabase
      .from('jobs')
      .insert([{
        id: jobId,
        user_id: userId,
        template_url: templateUrl,
        csv_storage_path: csvStoragePath,
        status: 'processing',
        certificate_count: 0,
        created_at: new Date().toISOString()
      }]);

    // 2. Download CSV
    jobEvents.emit('progress', {
      jobId,
      type: 'progress',
      percent: 10,
      message: 'Downloading CSV data...',
      stage: 'downloading_csv',
      processed: 0,
      total: 0,
      timestamp: new Date().toISOString()
    });

    const { data: csvData, error: csvError } = await supabase.storage
      .from('certificates')
      .download(csvStoragePath);

    if (csvError) throw new Error(`Failed to download CSV: ${csvError.message}`);

    const csvContent = await csvData.text();
    const lines = csvContent.split(/\r?\n/).filter(l => l.trim());
    
    if (lines.length < 2) {
      throw new Error('CSV must contain at least one data row');
    }
    
    const headers = lines[0].split(',').map(h => h.trim());
    participants = lines.slice(1).map(line => {
      const values = line.split(',').map(v => v.trim());
      const obj = {};
      headers.forEach((h, i) => obj[h] = values[i] || '');
      return obj;
    });

    // Update job with actual count
    await supabase
      .from('jobs')
      .update({
        certificate_count: participants.length
      })
      .eq('id', jobId);

    // 3. Download template
    jobEvents.emit('progress', {
      jobId,
      type: 'progress',
      percent: 20,
      message: 'Downloading template...',
      stage: 'downloading_template',
      processed: 0,
      total: participants.length,
      timestamp: new Date().toISOString()
    });

    let templateBuffer = null;
    if (templateUrl) {
      try {
        templateBuffer = await getBufferFromUrl(templateUrl);
      } catch (templateErr) {
        console.warn('Failed to download template:', templateErr.message);
      }
    }

    // 4. Load font
    const fontUrl = 'https://fonts.gstatic.com/s/roboto/v30/KFOmCnqEu92Fr1Mu4mxK.woff2';
    let fontBytes;
    try {
      fontBytes = Buffer.from(await (await fetch(fontUrl)).arrayBuffer());
    } catch (fontErr) {
      console.warn('Using default font:', fontErr.message);
      fontBytes = null;
    }

    // 5. Generate certificates CONCURRENTLY with proper batching
    jobEvents.emit('progress', {
      jobId,
      type: 'progress',
      percent: 30,
      message: `Generating ${participants.length} certificates concurrently...`,
      stage: 'generating',
      processed: 0,
      total: participants.length,
      timestamp: new Date().toISOString()
    });

    const pdfBuffers = [];
    const totalParticipants = participants.length;
    
    // Calculate optimal concurrency (adjust based on your system)
    const CONCURRENCY_LIMIT = 10; // Process 10 certificates at a time
    const BATCH_SIZE = 10; // Show progress after each batch
    
    // Helper function to generate a single certificate
    async function generateCertificate(participant, index) {
      try {
        const pdfDoc = await PDFDocument.create();
        
        if (fontBytes) {
          pdfDoc.registerFontkit(fontkit);
          await pdfDoc.embedFont(fontBytes);
        }
        
        const page = pdfDoc.addPage([600, 400]);

        // Add template image if available
        if (templateBuffer) {
          try {
            let img;
            if (templateUrl && templateUrl.endsWith('.png')) {
              img = await pdfDoc.embedPng(templateBuffer);
            } else {
              img = await pdfDoc.embedJpg(templateBuffer);
            }
            page.drawImage(img, { x: 0, y: 0, width: 600, height: 400 });
          } catch (imgErr) {
            console.warn('Failed to embed image:', imgErr.message);
          }
        }

        // Add text fields
        if (fields && Array.isArray(fields)) {
          fields.forEach(f => {
            const value = participant[f.field] || '';
            if (value && f.x !== undefined && f.y !== undefined) {
              try {
                const hex = (f.color || '#000000').replace('#', '');
                const r = parseInt(hex.slice(0, 2), 16) / 255;
                const g = parseInt(hex.slice(2, 4), 16) / 255;
                const b = parseInt(hex.slice(4, 6), 16) / 255;
                
                page.drawText(value.toString(), {
                  x: f.x,
                  y: 400 - f.y - (f.size || 16),
                  size: f.size || 16,
                  font: fontBytes ? undefined : pdfDoc.getFonts()[0],
                  color: rgb(r, g, b)
                });
              } catch (fieldErr) {
                console.warn(`Error drawing field ${f.field}:`, fieldErr.message);
              }
            }
          });
        }

        const pdfBytes = await pdfDoc.save();
        
        // Return result
        return {
          buffer: Buffer.from(pdfBytes),
          name: `${participant.name || participant[headers[0]] || `certificate_${index + 1}`}.pdf`,
          index: index
        };
      } catch (error) {
        console.error(`Error generating certificate ${index + 1}:`, error.message);
        // Return a fallback certificate
        const pdfDoc = await PDFDocument.create();
        const page = pdfDoc.addPage([600, 400]);
        page.drawText(`Certificate for ${participant.name || 'Participant'}`, {
          x: 50,
          y: 200,
          size: 20
        });
        const pdfBytes = await pdfDoc.save();
        
        return {
          buffer: Buffer.from(pdfBytes),
          name: `certificate_${index + 1}_error.pdf`,
          index: index,
          error: error.message
        };
      }
    }

    // Process participants in parallel with concurrency control
    for (let i = 0; i < totalParticipants; i += CONCURRENCY_LIMIT) {
      const batchStart = i;
      const batchEnd = Math.min(i + CONCURRENCY_LIMIT, totalParticipants);
      const currentBatch = participants.slice(batchStart, batchEnd);
      
      // Create promises for this batch
      const batchPromises = currentBatch.map((participant, batchIndex) => 
        generateCertificate(participant, batchStart + batchIndex)
      );
      
      // Process batch concurrently
      const batchResults = await Promise.all(batchPromises);
      
      // Add results to main array
      for (const result of batchResults) {
        pdfBuffers.push(result);
        
        // Update progress after each certificate
        const currentProgress = pdfBuffers.length;
        const percent = 30 + (currentProgress / totalParticipants * 60); // 30-90%
        
        jobEvents.emit('progress', {
          jobId,
          type: 'progress',
          percent: Math.round(percent),
          message: `Generated ${currentProgress} of ${totalParticipants} certificates`,
          stage: 'generating',
          processed: currentProgress,
          total: totalParticipants,
          timestamp: new Date().toISOString()
        });
      }
      
      // Small delay to prevent overwhelming the system
      if (i + CONCURRENCY_LIMIT < totalParticipants) {
        await new Promise(resolve => setTimeout(resolve, 100));
      }
      
      console.log(`✅ Processed batch ${Math.floor(i/CONCURRENCY_LIMIT) + 1}/${Math.ceil(totalParticipants/CONCURRENCY_LIMIT)}`);
    }
    
    // Sort by index to maintain order
    pdfBuffers.sort((a, b) => a.index - b.index);

    // 6. Create ZIP with progress updates
    jobEvents.emit('progress', {
      jobId,
      type: 'progress',
      percent: 90,
      message: 'Creating ZIP archive...',
      stage: 'archiving',
      processed: totalParticipants,
      total: totalParticipants,
      timestamp: new Date().toISOString()
    });

    // Create ZIP in memory
    const archive = archiver('zip', { 
      zlib: { level: 9 }
    });
    
    const zipBuffer = await new Promise((resolve, reject) => {
      const chunks = [];
      
      archive.on('data', (chunk) => chunks.push(chunk));
      archive.on('end', () => resolve(Buffer.concat(chunks)));
      archive.on('error', reject);
      
      // Add all PDFs to archive
      pdfBuffers.forEach(pdf => {
        archive.append(pdf.buffer, { name: pdf.name });
      });
      
      archive.finalize();
    });

    // 7. Store ZIP temporarily
    const zipFileName = `zips/${jobId}.zip`;
    await supabase.storage
      .from('certificates')
      .upload(zipFileName, zipBuffer, {
        contentType: 'application/zip',
        cacheControl: '3600',
      });

    const { data: urlData } = supabase.storage
      .from('certificates')
      .getPublicUrl(zipFileName);

    // 8. Update job status
    await supabase
      .from('jobs')
      .update({
        status: 'completed',
        download_url: urlData.publicUrl,
        updated_at: new Date().toISOString()
      })
      .eq('id', jobId);

    // 9. Emit completion
    jobEvents.emit('progress', {
      jobId,
      type: 'completed',
      percent: 100,
      message: `Generated ${totalParticipants} certificates successfully!`,
      stage: 'completed',
      processed: totalParticipants,
      total: totalParticipants,
      downloadUrl: urlData.publicUrl,
      timestamp: new Date().toISOString()
    });

    console.log(`✅ Generated ${totalParticipants} certificates for job ${jobId} using concurrent processing`);

  } catch (err) {
    console.error('Generation error:', err);
    
    // Update job status to failed
    await supabase
      .from('jobs')
      .update({
        status: 'failed',
        error_message: err.message,
        updated_at: new Date().toISOString()
      })
      .eq('id', jobId);

    // Emit error
    jobEvents.emit('progress', {
      jobId,
      type: 'error',
      percent: 0,
      message: `Generation failed: ${err.message}`,
      stage: 'failed',
      processed: 0,
      total: participants.length,
      timestamp: new Date().toISOString()
    });
  }
}

// ------------------------
// Alternative: Enhanced /generate-direct with concurrent processing
// ------------------------
app.post('/generate-direct-concurrent', upload.fields([
  { name: 'template', maxCount: 1 },
  { name: 'csv', maxCount: 1 }
]), async (req, res) => {
  const { userId, fields, concurrency = 5 } = req.body;
  const templateFile = req.files?.template?.[0];
  const csvFile = req.files?.csv?.[0];
  
  if (!userId || !csvFile || !templateFile) {
    return res.status(400).json({ 
      error: 'Missing required files. Need both template and CSV files.' 
    });
  }

  try {
    // Parse CSV
    const csvContent = csvFile.buffer.toString('utf8');
    const lines = csvContent.split(/\r?\n/).filter(l => l.trim());
    
    if (lines.length < 2) {
      return res.status(400).json({ error: 'CSV must contain at least one data row' });
    }
    
    const headers = lines[0].split(',').map(h => h.trim());
    const participants = lines.slice(1).map(line => {
      const values = line.split(',').map(v => v.trim());
      const obj = {};
      headers.forEach((h, i) => obj[h] = values[i] || '');
      return obj;
    });

    // Create job record
    const jobId = crypto.randomUUID();
    await supabase
      .from('jobs')
      .insert([{
        id: jobId,
        user_id: userId,
        status: 'processing',
        certificate_count: participants.length,
        created_at: new Date().toISOString()
      }]);

    // Load font
    const fontUrl = 'https://fonts.gstatic.com/s/roboto/v30/KFOmCnqEu92Fr1Mu4mxK.woff2';
    let fontBytes;
    try {
      fontBytes = Buffer.from(await (await fetch(fontUrl)).arrayBuffer());
    } catch (fontErr) {
      console.warn('Using default font:', fontErr.message);
      fontBytes = null;
    }

    // Set response headers
    res.setHeader('Content-Type', 'application/zip');
    res.setHeader('Content-Disposition', `attachment; filename="certificates_${Date.now()}.zip"`);
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('X-Job-ID', jobId);

    // Create ZIP archive
    const archive = archiver('zip', { zlib: { level: 9 } });
    archive.pipe(res);

    // Store certificate buffers
    const certificateBuffers = [];
    const CONCURRENCY = parseInt(concurrency) || 5;
    
    // Process in concurrent batches
    for (let i = 0; i < participants.length; i += CONCURRENCY) {
      const batch = participants.slice(i, i + CONCURRENCY);
      
      const batchPromises = batch.map(async (participant, batchIndex) => {
        const globalIndex = i + batchIndex;
        
        const pdfDoc = await PDFDocument.create();
        
        if (fontBytes) {
          pdfDoc.registerFontkit(fontkit);
          await pdfDoc.embedFont(fontBytes);
        }
        
        const page = pdfDoc.addPage([600, 400]);

        // Add template image
        try {
          let templateImage;
          if (templateFile.mimetype === 'image/png') {
            templateImage = await pdfDoc.embedPng(templateFile.buffer);
          } else {
            templateImage = await pdfDoc.embedJpg(templateFile.buffer);
          }
          page.drawImage(templateImage, { x: 0, y: 0, width: 600, height: 400 });
        } catch (imgErr) {
          console.warn('Failed to embed image:', imgErr.message);
        }

        // Add text fields
        if (fields && Array.isArray(fields)) {
          fields.forEach(f => {
            const value = participant[f.field] || '';
            if (value && f.x !== undefined && f.y !== undefined) {
              try {
                const hex = (f.color || '#000000').replace('#', '');
                const r = parseInt(hex.slice(0, 2), 16) / 255;
                const g = parseInt(hex.slice(2, 4), 16) / 255;
                const b = parseInt(hex.slice(4, 6), 16) / 255;
                
                page.drawText(value.toString(), {
                  x: f.x,
                  y: 400 - f.y - (f.size || 16),
                  size: f.size || 16,
                  font: fontBytes ? undefined : pdfDoc.getFonts()[0],
                  color: rgb(r, g, b)
                });
              } catch (fieldErr) {
                console.warn(`Error drawing field ${f.field}:`, fieldErr.message);
              }
            }
          });
        }

        const pdfBytes = await pdfDoc.save();
        const participantName = participant.name || participant[headers[0]] || `certificate_${globalIndex + 1}`;
        const safeFileName = `${participantName.replace(/[^a-zA-Z0-9]/g, '_')}.pdf`;
        
        return {
          buffer: Buffer.from(pdfBytes),
          name: safeFileName,
          index: globalIndex
        };
      });

      // Process batch concurrently
      const batchResults = await Promise.all(batchPromises);
      certificateBuffers.push(...batchResults);
      
      // Log progress
      console.log(`Processed batch ${Math.floor(i/CONCURRENCY) + 1}/${Math.ceil(participants.length/CONCURRENCY)}`);
    }

    // Sort by index and add to archive
    certificateBuffers
      .sort((a, b) => a.index - b.index)
      .forEach(cert => {
        archive.append(cert.buffer, { name: cert.name });
      });

    await archive.finalize();
    
    // Update job status
    await supabase
      .from('jobs')
      .update({
        status: 'completed',
        updated_at: new Date().toISOString()
      })
      .eq('id', jobId);

    console.log(`✅ Generated ${participants.length} certificates concurrently for user ${userId}`);

  } catch (err) {
    console.error('Direct generation error:', err);
    
    if (!res.headersSent) {
      res.status(500).json({ 
        error: 'Generation failed',
        details: process.env.NODE_ENV === 'development' ? err.message : undefined
      });
    }
  }
});
// ------------------------
// Alternative: Generate from direct file uploads (no storage)
// ------------------------
app.post('/generate-direct', upload.fields([
  { name: 'template', maxCount: 1 },
  { name: 'csv', maxCount: 1 }
]), async (req, res) => {
  const { userId, fields } = req.body;
  const templateFile = req.files?.template?.[0];
  const csvFile = req.files?.csv?.[0];
  
  if (!userId || !csvFile || !templateFile) {
    return res.status(400).json({ 
      error: 'Missing required files. Need both template and CSV files.' 
    });
  }

  try {
    // Parse CSV
    const csvContent = csvFile.buffer.toString('utf8');
    const lines = csvContent.split(/\r?\n/).filter(l => l.trim());
    
    if (lines.length < 2) {
      return res.status(400).json({ error: 'CSV must contain at least one data row' });
    }
    
    const headers = lines[0].split(',').map(h => h.trim());
    const participants = lines.slice(1).map(line => {
      const values = line.split(',').map(v => v.trim());
      const obj = {};
      headers.forEach((h, i) => obj[h] = values[i] || '');
      return obj;
    });

    // Create job record
    const jobId = crypto.randomUUID();
    await supabase
      .from('jobs')
      .insert([{
        id: jobId,
        user_id: userId,
        status: 'processing',
        certificate_count: participants.length,
        created_at: new Date().toISOString()
      }]);

    // Set response headers
    res.setHeader('Content-Type', 'application/zip');
    res.setHeader('Content-Disposition', `attachment; filename="certificates_${Date.now()}.zip"`);
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('X-Job-ID', jobId);

    // Create ZIP archive
    const archive = archiver('zip', { zlib: { level: 9 } });
    archive.pipe(res);

    // Load font
    const fontUrl = 'https://fonts.gstatic.com/s/roboto/v30/KFOmCnqEu92Fr1Mu4mxK.woff2';
    let fontBytes;
    try {
      fontBytes = Buffer.from(await (await fetch(fontUrl)).arrayBuffer());
    } catch (fontErr) {
      console.warn('Using default font:', fontErr.message);
      fontBytes = null;
    }

    // Generate certificates
    for (let i = 0; i < participants.length; i++) {
      const participant = participants[i];
      const pdfDoc = await PDFDocument.create();
      
      if (fontBytes) {
        pdfDoc.registerFontkit(fontkit);
        await pdfDoc.embedFont(fontBytes);
      }
      
      const page = pdfDoc.addPage([600, 400]);

      // Add template image
      try {
        let templateImage;
        if (templateFile.mimetype === 'image/png') {
          templateImage = await pdfDoc.embedPng(templateFile.buffer);
        } else {
          templateImage = await pdfDoc.embedJpg(templateFile.buffer);
        }
        page.drawImage(templateImage, { x: 0, y: 0, width: 600, height: 400 });
      } catch (imgErr) {
        console.warn('Failed to embed image:', imgErr.message);
      }

      // Add text fields
      if (fields && Array.isArray(fields)) {
        fields.forEach(f => {
          const value = participant[f.field] || '';
          if (value && f.x !== undefined && f.y !== undefined) {
            try {
              const hex = (f.color || '#000000').replace('#', '');
              const r = parseInt(hex.slice(0, 2), 16) / 255;
              const g = parseInt(hex.slice(2, 4), 16) / 255;
              const b = parseInt(hex.slice(4, 6), 16) / 255;
              
              page.drawText(value.toString(), {
                x: f.x,
                y: 400 - f.y - (f.size || 16),
                size: f.size || 16,
                font: fontBytes ? undefined : pdfDoc.getFonts()[0],
                color: rgb(r, g, b)
              });
            } catch (fieldErr) {
              console.warn(`Error drawing field ${f.field}:`, fieldErr.message);
            }
          }
        });
      }

      const pdfBytes = await pdfDoc.save();
      const participantName = participant.name || participant[headers[0]] || `certificate_${i + 1}`;
      const safeFileName = `${participantName.replace(/[^a-zA-Z0-9]/g, '_')}.pdf`;
      
      archive.append(Buffer.from(pdfBytes), { name: safeFileName });
    }

    await archive.finalize();
    
    // Update job status
    await supabase
      .from('jobs')
      .update({
        status: 'completed',
        updated_at: new Date().toISOString()
      })
      .eq('id', jobId);

    console.log(`✅ Generated ${participants.length} certificates directly for user ${userId}`);

  } catch (err) {
    console.error('Direct generation error:', err);
    
    if (!res.headersSent) {
      res.status(500).json({ 
        error: 'Generation failed',
        details: process.env.NODE_ENV === 'development' ? err.message : undefined
      });
    }
  }
});

// ------------------------
// Get user jobs (history)
// ------------------------
app.get('/jobs/:userId', async (req, res) => {
  const userId = req.params.userId;
  
  try {
    const { data, error } = await supabase
      .from('jobs')
      .select('id, user_id, template_url, status, created_at, certificate_count, csv_storage_path')
      .eq('user_id', userId)
      .order('created_at', { ascending: false });

    if (error) {
      console.error('Jobs fetch error:', error);
      return res.status(500).json({ error: 'Failed to fetch jobs' });
    }

    res.json(data || []);
  } catch (err) {
    console.error('Jobs endpoint error:', err);
    res.status(500).json({ error: 'Failed to fetch jobs' });
  }
});

// ------------------------
// Delete template from storage
// ------------------------
app.delete('/delete/template/:storagePath', async (req, res) => {
  const storagePath = req.params.storagePath;
  const { userId } = req.body;

  if (!userId) {
    return res.status(400).json({ error: 'User ID required' });
  }

  try {
    // Delete from Supabase Storage
    const { data: deleteData, error: deleteError } = await supabase.storage
      .from('certificates')
      .remove([storagePath]);

    if (deleteError) {
      console.error('Storage delete error:', deleteError);
      return res.status(500).json({ error: 'Failed to delete template from storage' });
    }

    res.json({ 
      success: true, 
      message: 'Template deleted successfully',
      deletedPath: storagePath
    });

  } catch (err) {
    console.error('Delete template error:', err);
    res.status(500).json({ error: 'Failed to delete template' });
  }
});

// ------------------------
// Delete CSV from storage
// ------------------------
app.delete('/delete/csv/:storagePath', async (req, res) => {
  const storagePath = req.params.storagePath;
  const { userId } = req.body;

  if (!userId) {
    return res.status(400).json({ error: 'User ID required' });
  }

  try {
    const { data: deleteData, error: deleteError } = await supabase.storage
      .from('certificates')
      .remove([storagePath]);

    if (deleteError) {
      console.error('Storage delete error:', deleteError);
      return res.status(500).json({ error: 'Failed to delete CSV from storage' });
    }

    res.json({ 
      success: true, 
      message: 'CSV deleted successfully',
      deletedPath: storagePath
    });

  } catch (err) {
    console.error('Delete CSV error:', err);
    res.status(500).json({ error: 'Failed to delete CSV' });
  }
});

// ------------------------
// Delete job history
// ------------------------
app.delete('/jobs/:jobId', async (req, res) => {
  const jobId = req.params.jobId;
  const { userId } = req.body;

  if (!userId) {
    return res.status(400).json({ error: 'User ID required' });
  }

  try {
    // Verify user ownership
    const { valid, error } = await verifyUserOwnership(userId, jobId);
    if (!valid) {
      return res.status(403).json({ error: error || 'Unauthorized' });
    }

    // Delete from database only (no ZIP files to delete)
    const { error: deleteError } = await supabase
      .from('jobs')
      .delete()
      .eq('id', jobId);

    if (deleteError) throw deleteError;

    res.json({ 
      success: true, 
      message: 'Job history deleted'
    });
  } catch (err) {
    console.error('Delete error:', err);
    res.status(500).json({ error: 'Failed to delete job' });
  }
});

// ------------------------
// Get user's stored templates
// ------------------------
app.get('/templates/:userId', async (req, res) => {
  const userId = req.params.userId;
  
  try {
    // Get jobs that have templates
    const { data, error } = await supabase
      .from('jobs')
      .select('id, template_url, csv_storage_path, created_at')
      .eq('user_id', userId)
      .not('template_url', 'is', null)
      .order('created_at', { ascending: false });

    if (error) {
      console.error('Templates fetch error:', error);
      return res.status(500).json({ error: 'Failed to fetch templates' });
    }

    res.json(data || []);
  } catch (err) {
    console.error('Templates endpoint error:', err);
    res.status(500).json({ error: 'Failed to fetch templates' });
  }
});

// ------------------------
// Health check
// ------------------------
app.get('/health', async (req, res) => {
  try {
    const { data, error } = await supabase.from('users').select('count').limit(1);
    
    res.json({ 
      status: 'healthy',
      supabase: error ? 'disconnected' : 'connected',
      timestamp: new Date().toISOString()
    });
  } catch (err) {
    res.status(503).json({ 
      status: 'unhealthy',
      supabase: 'disconnected',
      error: err.message
    });
  }
});

// ------------------------
// Start server
// ------------------------
app.listen(port, () => {
  console.log(`✅ Server running at http://localhost:${port}`);
  console.log(`☁️  Using Supabase for template/CSV storage only`);
  console.log(`📦 ZIP files are NOT stored - sent directly to user`);
  console.log(`📊 Available endpoints:`);
  console.log(`   POST /upload-template (store template)`);
  console.log(`   POST /upload-csv (store CSV)`);
  console.log(`   POST /generate (use stored files, direct ZIP download)`);
  console.log(`   POST /generate-direct (direct upload, no storage)`);
  console.log(`   GET  /jobs/:userId (job history)`);
  console.log(`   GET  /templates/:userId (stored templates)`);
  console.log(`   DELETE /delete/template/:storagePath`);
  console.log(`   DELETE /delete/csv/:storagePath`);
  console.log(`   DELETE /jobs/:jobId (delete history)`);
});
