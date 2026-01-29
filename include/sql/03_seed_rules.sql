use database scam_bank_db;
use schema fraud_intel_ref;

insert into typology_rules (typology_id, pattern, weight) values
('bec', '(business email compromise|\\bbec\\b|invoice scam|vendor payment)', 3),
('impersonation', '(impersonat|spoof|pretend to be|fake representative)', 2),
('investment', '(investment scam|guaranteed returns|crypto investment|pig butchering)', 3),
('romance', '(romance scam|dating app|online relationship)', 3),
('phishing', '(phish|credential|login page|spoofed site)', 2);

insert into signal_rules (signal_type, signal_value, pattern, weight) values
('payment', 'gift_cards', '(gift card|itunes|google play|steam card)', 3),
('payment', 'wire', '(wire transfer|swift|bank wire)', 3),
('payment', 'crypto', '(crypto|bitcoin|wallet address|usdt)', 3),
('channel', 'email', '(email|inbox|phishing)', 2),
('channel', 'sms', '(sms|text message|smishing)', 2),
('channel', 'phone', '(call you|phone|vishing)', 2),
('red_flag', 'urgency', '(urgent|immediately|act now|right away)', 2),
('red_flag', 'secrecy', '(do not tell|keep this secret|confidential)', 2);
