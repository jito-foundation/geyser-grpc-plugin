//! Code from: https://github.com/ckamm/solana-accountsdb-connector/blob/master/geyser-plugin-grpc/src/accounts_selector.rs#L1

use std::collections::HashSet;

use log::*;
use serde_json::Value;

#[derive(Debug)]
pub(crate) struct AccountsSelector {
    pub accounts: HashSet<Vec<u8>>,
    pub owners: HashSet<Vec<u8>>,
    pub select_all_accounts: bool,
}

impl AccountsSelector {
    pub fn default() -> Self {
        AccountsSelector {
            accounts: HashSet::default(),
            owners: HashSet::default(),
            select_all_accounts: true,
        }
    }

    pub fn new(accounts: &[String], owners: &[String]) -> Self {
        info!(
            "Creating AccountsSelector from accounts: {:?}, owners: {:?}",
            accounts, owners
        );

        let select_all_accounts = accounts.iter().any(|key| key == "*");
        if select_all_accounts {
            return AccountsSelector {
                accounts: HashSet::default(),
                owners: HashSet::default(),
                select_all_accounts,
            };
        }

        let accounts = accounts
            .iter()
            .map(|key| bs58::decode(key).into_vec().unwrap())
            .collect();
        let owners = owners
            .iter()
            .map(|key| bs58::decode(key).into_vec().unwrap())
            .collect();

        AccountsSelector {
            accounts,
            owners,
            select_all_accounts,
        }
    }

    pub fn is_account_selected(&self, account: &[u8], owner: &[u8]) -> bool {
        self.select_all_accounts || self.accounts.contains(account) || self.owners.contains(owner)
    }
}

impl From<&Value> for AccountsSelector {
    fn from(json: &Value) -> Self {
        if json.is_null() {
            return AccountsSelector::default();
        }

        let deserialize_accounts = |accounts: &Value| -> Vec<String> {
            if accounts.is_array() {
                accounts
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|val| val.as_str().unwrap().to_string())
                    .collect()
            } else {
                vec![]
            }
        };

        let accounts = deserialize_accounts(&json["accounts"]);
        let owners = deserialize_accounts(&json["owners"]);
        AccountsSelector::new(&accounts[..], &owners[..])
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    #[test]
    fn test_create_accounts_selector() {
        AccountsSelector::new(
            &["9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin".to_string()],
            &[],
        );

        AccountsSelector::new(
            &[],
            &["9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin".to_string()],
        );
    }
}
