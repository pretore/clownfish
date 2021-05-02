use std::collections::HashMap;
use crate::uri::Uri;


#[derive(Debug)]
pub struct Configuration {

}

impl Configuration {
    pub fn new<F>(
        uri: Uri,
        callback: Option<F>
    ) -> Self
        where
            F: FnMut(&HashMap<String, String>) + Send + 'static
    {
        Configuration {

        }
    }
}

#[cfg(test)]
mod tests {

}