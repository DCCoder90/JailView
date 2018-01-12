BOT_NAME = 'jailview'

SPIDER_MODULES = ['jailview.spiders']
NEWSPIDER_MODULE = 'jailview.spiders'

DBU = 'DATABASEUSER'
DBP = 'DATABASEPASSWORD'
DB = 'DATABASENAME'
DBH = 'localhost'
DBPORT = 3306

USER_AGENT = 'IntelNexus Spider (USER@EXAMPLE.COM)'

TELNETCONSOLE_ENABLED=False

ITEM_PIPELINES = {
    'jailview.pipelines.MySQLPipeline': 300,
}