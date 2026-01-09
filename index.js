import cron from "node-cron";
import { exec } from "child_process";
import fs from "fs";
import path from "path";
import dotenv from "dotenv";
import cloudinary from "cloudinary";
import nodemailer from "nodemailer";

dotenv.config();

/* ================= CONFIG ================= */
const {
  MONGO_URI, // Source cluster
  BACKUP_MONGO_URI, // Backup cluster (user must have Atlas admin role!)
  SOURCE_DB_NAME = "crm_backend",
  BACKUP_DIR = process.env.BACKUP_DIR || "/root/sensio-backup-db",
  RETENTION_DAYS = "7",
  CLOUDINARY_CLOUD_NAME,
  CLOUDINARY_API_KEY,
  CLOUDINARY_API_SECRET,
  EMAIL_HOST,
  EMAIL_PORT,
  EMAIL_USER,
  EMAIL_PASS,
  ALERT_EMAIL,
} = process.env;

/* ================= TOOLS PATH (UBUNTU) ================= */
const MONGODUMP_PATH = process.env.MONGODUMP_PATH || "mongodump";
const MONGORESTORE_PATH = process.env.MONGORESTORE_PATH || "mongorestore";

/* ================= HELPERS ================= */
const maskUri = (uri) =>
  uri ? uri.replace(/\/\/(.*?):(.*?)@/, "//****:****@") : "NOT SET";

/* ================= CLOUDINARY ================= */
cloudinary.v2.config({
  cloud_name: CLOUDINARY_CLOUD_NAME,
  api_key: CLOUDINARY_API_KEY,
  api_secret: CLOUDINARY_API_SECRET,
});

/* ================= EMAIL ================= */
const transporter = nodemailer.createTransport({
  host: EMAIL_HOST,
  port: Number(EMAIL_PORT),
  secure: false,
  auth: { user: EMAIL_USER, pass: EMAIL_PASS },
});

const sendEmail = async (subject, text) => {
  try {
    await transporter.sendMail({
      from: `"Backup Alert" <${EMAIL_USER}>`,
      to: ALERT_EMAIL,
      subject,
      text: `${text}\n\nTime: ${new Date().toLocaleString()}`,
    });
    console.log("📧 Email sent");
  } catch (e) {
    console.error("Email send failed:", e.message);
  }
};

/* ================= BACKUP FUNCTION ================= */
const runBackup = async (trigger = "cron") => {
  console.log("🔎 SOURCE:", maskUri(MONGO_URI));
  console.log("🔎 BACKUP DB:", maskUri(BACKUP_MONGO_URI));

  const now = new Date();
  const date = now.toISOString().split("T")[0];
  const hour = String(now.getHours()).padStart(2, "0");
  const minute = String(now.getMinutes()).padStart(2, "0");
  const timestamp = `${date}_${hour}-${minute}`;

  const backupPath = path.join(BACKUP_DIR, date, hour);
  fs.mkdirSync(backupPath, { recursive: true });
  const archiveFile = path.join(backupPath, `backup_${timestamp}.gz`);

  console.log(`⏳ [${trigger.toUpperCase()}] Starting backup...`);

  // Step 1: Create file backup
  const dumpCmd = `${MONGODUMP_PATH} --uri="${MONGO_URI}" --archive="${archiveFile}" --gzip`;

  exec(dumpCmd, async (dumpErr) => {
    if (dumpErr) {
      console.error("❌ File backup failed:", dumpErr.message);
      await sendEmail(
        "❌ MongoDB Backup Failed",
        `File backup error:\n${dumpErr.message}`
      );
      return;
    }

    console.log("✅ File backup created:", archiveFile);

    // Upload to Cloudinary
    try {
      await cloudinary.v2.uploader.upload(archiveFile, {
        resource_type: "raw",
        folder: "mongodb-backups",
        public_id: `backup_${timestamp}`,
      });
      console.log("☁️ Uploaded to Cloudinary");
    } catch (uploadErr) {
      console.warn("Cloudinary upload failed:", uploadErr.message);
    }

    // Step 2: Replicate to backup DB (if URI provided)
    if (BACKUP_MONGO_URI) {
      console.log("🔁 Replicating to backup cluster...");
      const replicateCmd = `${MONGODUMP_PATH} --uri="${MONGO_URI}" --archive --gzip | ${MONGORESTORE_PATH} --uri="${BACKUP_MONGO_URI}" --archive --gzip --drop --numParallelCollections=4`;

      exec(replicateCmd, async (repErr, stdout) => {
        if (repErr) {
          console.error("❌ Replication failed:", repErr.message);
          await sendEmail(
            "❌ MongoDB Backup Failed",
            `Replication error:\n${repErr.message}\n\nOutput:\n${
              stdout || "none"
            }`
          );
          return;
        }

        console.log("✅ Backup saved to MongoDB cluster!");
        await sendEmail(
          "✅ MongoDB Backup Successful",
          `Backup completed\nFile: ${archiveFile}\nDB Replication: Success\nTrigger: ${trigger}`
        );
      });
    } else {
      await sendEmail(
        "✅ MongoDB File Backup Successful",
        `File backup only\nFile: ${archiveFile}`
      );
    }

    // Cleanup old backups
    const retentionMs = parseInt(RETENTION_DAYS) * 24 * 60 * 60 * 1000;
    try {
      fs.readdirSync(BACKUP_DIR).forEach((dateFolder) => {
        const fullPath = path.join(BACKUP_DIR, dateFolder);
        if (fs.statSync(fullPath).mtimeMs < Date.now() - retentionMs) {
          fs.rmSync(fullPath, { recursive: true, force: true });
          console.log("🗑️ Removed old:", dateFolder);
        }
      });
    } catch (e) {}
  });
};

/* ================= SCHEDULER ================= */
cron.schedule("0 * * * *", () => runBackup("cron")); // Every hour

if (process.argv.includes("now")) {
  runBackup("manual");
}

console.log("⏰ MongoDB backup service running...");
