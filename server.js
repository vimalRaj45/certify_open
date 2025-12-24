import express from 'express';
import multer from 'multer';
import archiver from 'archiver';
import { PDFDocument, rgb } from 'pdf-lib';
import { v2 as cloudinary } from 'cloudinary';
import dotenv from 'dotenv';
import fetch from 'node-fetch';
import path from 'path';
import fs from 'fs';
import fontkit from '@pdf-lib/fontkit';
import cors from 'cors';
import bcrypt from 'bcrypt';
import pkg from 'pg';
import { fileURLToPath } from 'url';

dotenv.config();
const { Pool } = pkg;

// ------------------------
// PostgreSQL
// ------------------------
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false, // required for Supabase
  },
});

// Example: test connection
pool.query('SELECT NOW()', (err, res) => {
  if (err) console.error('DB Connection Error:', err);
  else console.log('DB Connected:', res.rows[0]);
});
// ------------------------
// Cloudinary
// ------------------------
cloudinary.config({
  cloud_name: process.env.CLOUDINARY_CLOUD_NAME,
  api_key: process.env.CLOUDINARY_API_KEY,
  api_secret: process.env.CLOUDINARY_API_SECRET,
  secure: true,
});

// ------------------------
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const app = express();
const port = process.env.PORT || 5000;

app.use(cors({
  origin: 'http://localhost:3000',
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization']
}));
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true }));

const upload = multer({ storage: multer.memoryStorage() });

// ------------------------
// SSE clients storage
// ------------------------
let clients = [];

// ------------------------
// Helper: fetch image
// ------------------------
async function getBufferFromUrl(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error('Failed to fetch image');
  return Buffer.from(await res.arrayBuffer());
}

// ------------------------
// Auth
// ------------------------
app.post('/signup', async (req, res) => {
  const { email, password } = req.body;
  if (!email || !password) return res.status(400).json({ error: 'Missing fields' });

  const hashed = await bcrypt.hash(password, 10);

  try {
    const result = await pool.query(
      'INSERT INTO users(email, password) VALUES($1, $2) RETURNING id,email',
      [email, hashed]
    );
    res.json({ success: true, user: result.rows[0] });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Signup failed' });
  }
});

app.post('/login', async (req,res)=>{
  const { email, password } = req.body;
  if(!email||!password) return res.status(400).json({error:'Missing fields'});
  try{
    const result = await pool.query('SELECT * FROM users WHERE email=$1',[email]);
    const user = result.rows[0];
    if(!user) return res.status(401).json({error:'Invalid credentials'});

    const match = await bcrypt.compare(password,user.password);
    if(!match) return res.status(401).json({error:'Invalid credentials'});

    res.json({success:true,user:{id:user.id,email:user.email}});
  }catch(err){
    console.error(err);
    res.status(500).json({error:'Login failed'});
  }
});

// ------------------------
// SSE Progress
// ------------------------
app.get('/progress/:jobId', (req,res)=>{
  const jobId = req.params.jobId;
  res.setHeader('Content-Type','text/event-stream');
  res.setHeader('Cache-Control','no-cache');
  res.setHeader('Connection','keep-alive');
  res.flushHeaders();

  const client = { id: Date.now()+Math.random(), jobId, res };
  clients.push(client);

  req.on('close', ()=>{
    clients = clients.filter(c=>c.id!==client.id);
  });
});

function sendProgress(jobId,data){
  clients.forEach(c=>{
    if(c.jobId===jobId){
      c.res.write(`data: ${JSON.stringify(data)}\n\n`);
    }
  });
}

// ------------------------
// Upload template
// ------------------------
app.post('/upload-template', upload.single('template'), async (req,res)=>{
  if(!req.file) return res.status(400).json({error:'No file'});
  try{
    const result = await new Promise((resolve,reject)=>{
      const stream = cloudinary.uploader.upload_stream({resource_type:'image'}, (err,res)=>{
        err?reject(err):resolve(res);
      });
      stream.end(req.file.buffer);
    });
    res.json({templateUrl:result.secure_url});
  }catch(err){
    console.error(err);
    res.status(500).json({error:'Upload failed'});
  }
});

// ------------------------
// Upload CSV
// ------------------------
app.post('/upload-csv', upload.single('csv'), async (req,res)=>{
  if(!req.file) return res.status(400).json({error:'No CSV'});
  const fileName = `temp/csv_${Date.now()}.csv`;
  fs.writeFileSync(fileName, req.file.buffer);
  res.json({csvPath:fileName});
});

// ------------------------
// Generate PDFs + ZIP
// ------------------------
app.post('/generate', async (req,res)=>{
  const { userId, csvPath, templateUrl, fields } = req.body;
  if(!userId||!csvPath) return res.status(400).json({error:'Missing params'});

  const jobId = `job_${Date.now()}_${Math.random().toString(36).slice(2,10)}`;
  res.json({success:true, jobId});

  try{
    const content = fs.readFileSync(csvPath,'utf8');
    const lines = content.split(/\r?\n/).filter(l=>l.trim());
    const headers = lines[0].split(',').map(h=>h.trim());
    const participants = lines.slice(1).map(line=>{
      const values = line.split(',').map(v=>v.trim());
      const obj={};
      headers.forEach((h,i)=>obj[h]=values[i]||'');
      return obj;
    });

    const zipPath = path.join(__dirname,`temp/zip_${jobId}.zip`);
    const output = fs.createWriteStream(zipPath);
    const archive = archiver('zip',{zlib:{level:9}});
    archive.pipe(output);

    const fontUrl = 'https://github.com/googlefonts/noto-fonts/raw/main/hinted/ttf/NotoSans/NotoSans-Regular.ttf';
    const fontBytes = Buffer.from(await (await fetch(fontUrl)).arrayBuffer());

    const BATCH_SIZE = 5;
    let processed = 0;

    for(let start=0;start<participants.length;start+=BATCH_SIZE){
      const batch = participants.slice(start,start+BATCH_SIZE);
      await Promise.all(batch.map(async p=>{
        const pdfDoc = await PDFDocument.create();
        pdfDoc.registerFontkit(fontkit);
        const font = await pdfDoc.embedFont(fontBytes);
        const page = pdfDoc.addPage([600,400]);

        if(templateUrl){
          const imgBytes = await getBufferFromUrl(templateUrl);
          let img;
          if(templateUrl.endsWith('.png')) img = await pdfDoc.embedPng(imgBytes);
          else img = await pdfDoc.embedJpg(imgBytes);
          page.drawImage(img,{x:0,y:0,width:600,height:400});
        }

        fields?.forEach(f=>{
          const value = p[f.field]||'';
          const hex = (f.color||'#000000').replace('#','');
          page.drawText(value.toString(),{
            x:f.x,
            y:400-f.y-f.size,
            size:f.size,
            font,
            color: rgb(parseInt(hex.slice(0,2),16)/255, parseInt(hex.slice(2,4),16)/255, parseInt(hex.slice(4,6),16)/255)
          });
        });

        const pdfBytes = await pdfDoc.save();
        archive.append(Buffer.from(pdfBytes),{name:`${p.name||'user'}.pdf`});

        processed++;
        sendProgress(jobId,{processed,total:participants.length,percent:Math.round((processed/participants.length)*100)});
      }));
    }

    await archive.finalize();
    await pool.query(
      'INSERT INTO jobs(user_id,template_url,csv_path,zip_path,status) VALUES($1,$2,$3,$4,$5)',
      [userId, templateUrl, csvPath, zipPath, 'completed']
    );

    output.on('close',()=>sendProgress(jobId,{completed:true,downloadUrl:`/download/${jobId}`}));

  }catch(err){
    console.error(err);
    sendProgress(jobId,{error:'Generation failed'});
  }
});

// ------------------------
// Download ZIP
// ------------------------
// Add this for download endpoint
app.get('/download/:jobId', async (req, res) => {
  const jobId = req.params.jobId;
  const result = await pool.query('SELECT * FROM jobs WHERE zip_path LIKE $1',['%'+jobId+'%']);
  const job = result.rows[0];
  if(!job || !fs.existsSync(job.zip_path)) return res.status(404).send('ZIP not found');
  
  // Set proper headers for download
  res.setHeader('Content-Type', 'application/zip');
  res.setHeader('Content-Disposition', `attachment; filename="certificates_${jobId}.zip"`);
  res.download(job.zip_path, `certificates_${jobId}.zip`, (err)=>{
    if(err) console.error('Download error:', err);
  });
});

// ------------------------
app.listen(port,()=>console.log(`Server running at http://localhost:${port}`));
