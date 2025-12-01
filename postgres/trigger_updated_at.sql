DROP TRIGGER IF EXISTS update_accounts_modtime ON accounts;

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW(); 
    RETURN NEW; 
END;
$$ language 'plpgsql';


CREATE TRIGGER update_accounts_modtime
    BEFORE UPDATE ON accounts
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();