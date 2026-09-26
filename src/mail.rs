use resend_rs::types::CreateEmailBaseOptions;
use resend_rs::Resend;

pub struct Mailer {
    resend: Resend,
    from: String,
}

impl Mailer {
    pub fn new(api_key: String, from: String) -> Self {
        Self {
            resend: Resend::new(&api_key),
            from,
        }
    }

    pub async fn send_html(&self, to: &str, subject: &str, html: &str) -> Result<(), ()> {
        let email = CreateEmailBaseOptions::new(&self.from, [to], subject).with_html(html);
        self.resend
            .emails
            .send(email)
            .await
            .map(|_| ())
            .map_err(|err| {
                tracing::warn!("resend: {err}");
            })
    }
}
