import nodemailer from 'nodemailer';

// Email transporter using Google Workspace SMTP (TLS)
const transporter = nodemailer.createTransport({
  host: process.env.SMTP_HOST || 'smtp.gmail.com',
  port: 465,
  secure: true,
  auth: {
    user: process.env.SMTP_USER,
    pass: process.env.SMTP_PASS?.replace(/\s/g, ''),
  },
});


interface ScrapeEmailData {
  to: string;
  platform: string;
  target: string;
  count: number;
  cost: number;
  fileName: string;
  fileBuffer: Buffer;
}

export async function sendScrapeResultEmail(data: ScrapeEmailData): Promise<boolean> {
  const { to, platform, target, count, cost, fileName, fileBuffer } = data;

  const html = `
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Your Data is Ready</title>
</head>
<body style="margin: 0; padding: 0; background-color: #f0f0f0; font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif; color: #000000;">
  <table width="100%" border="0" cellspacing="0" cellpadding="0" style="background-color: #f0f0f0;">
    <tr>
      <td align="center" style="padding: 60px 20px;">
        <table width="600" border="0" cellspacing="0" cellpadding="0" style="background-color: #ffffff; border: 4px solid #000000; box-shadow: 12px 12px 0px #000000; max-width: 100%;">
          <tr>
            <td align="left" style="background-color: #000000; padding: 15px 30px;">
               <div style="font-size: 14px; font-weight: 900; color: #ffffff; letter-spacing: 2px; text-transform: uppercase;">
                 FEED ME / DATA DELIVERY
               </div>
            </td>
          </tr>
          <tr>
            <td align="center" style="padding: 50px 40px;">
              <div style="display: inline-block; background-color: #ccff00; color: #000000; padding: 8px 16px; font-size: 12px; font-weight: 900; letter-spacing: 1px; border: 2px solid #000000; margin-bottom: 30px; text-transform: uppercase;">
                MISSION COMPLETE
              </div>
              <h1 style="margin: 0 0 20px 0; font-size: 42px; line-height: 1; font-weight: 900; color: #000000; text-transform: uppercase; letter-spacing: -1px;">
                ${platform.toUpperCase()} <br>SECURED.
              </h1>
              <table width="100%" border="0" cellspacing="0" cellpadding="0" style="margin: 30px 0;">
                <tr>
                   <td align="center" width="50%" style="padding: 15px; border: 2px solid #eeeeee;">
                      <div style="font-size: 24px; font-weight: 900; color: #000000;">${count}</div>
                      <div style="font-size: 10px; font-weight: 700; color: #999999; text-transform: uppercase; letter-spacing: 1px;">POSTS</div>
                   </td>
                   <td align="center" width="50%" style="padding: 15px; border: 2px solid #eeeeee;">
                      <div style="font-size: 24px; font-weight: 900; color: #000000;">₹${cost.toFixed(2)}</div>
                      <div style="font-size: 10px; font-weight: 700; color: #999999; text-transform: uppercase; letter-spacing: 1px;">CREDITS</div>
                   </td>
                </tr>
              </table>
              <p style="font-size: 16px; line-height: 1.6; font-weight: 500; color: #333333; margin: 0 0 20px 0; max-width: 400px;">
                Your data from <strong>${target}</strong> is attached as an Excel file. Zero hassle, zero expiry.
              </p>
              <p style="font-size: 12px; color: #666666;">
                📎 <strong>${fileName}</strong> attached below
              </p>
            </td>
          </tr>
          <tr>
            <td align="center" style="border-top: 4px solid #000000; background-color: #f9f9f9; padding: 20px;">
              <p style="margin: 0; font-size: 12px; font-weight: 700; color: #cccccc; text-transform: uppercase;">
                FEED ME © 2026
              </p>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>
  `;

  try {
    await transporter.sendMail({
      from: `"Feed Me" <${process.env.SMTP_USER}>`,
      to,
      subject: `Your ${platform.toUpperCase()} data is ready! 📊`,
      html,
      attachments: [
        {
          filename: fileName,
          content: fileBuffer,
          contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        },
      ],
    });
    return true;
  } catch (error) {
    console.error('Failed to send email:', error);
    return false;
  }
}
