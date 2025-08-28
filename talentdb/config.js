module.exports = {
  mongodb: {
    server: process.env.ME_CONFIG_MONGODB_SERVER || 'localhost',
    port: parseInt(process.env.ME_CONFIG_MONGODB_PORT || '27017', 10),
    ssl: false,
    auth: [],
    admin: true,
    authSource: null,
    useUnifiedTopology: true,
    database: process.env.DB_NAME || 'talent_match'
  },
  site: {
    port: parseInt(process.env.ME_CONFIG_BASICAUTH_PORT || '8081', 10),
    host: '0.0.0.0',
    cookieSecret: 'changeme-cookie',
    sessionSecret: 'changeme-session',
    sslEnabled: false,
    url: '/',
  },
  basicAuth: {
    username: process.env.ME_CONFIG_BASICAUTH_USERNAME || 'admin',
    password: process.env.ME_CONFIG_BASICAUTH_PASSWORD || 'pass'
  },
  options: {
    documentsPerPage: 50,
    editorTheme: 'default',
    readOnly: false,
  }
};
