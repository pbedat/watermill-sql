package notify

import (
	"fmt"

	wm_sql "github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
)

type NotifySchemaDecorator struct {
	wm_sql.DefaultPostgreSQLSchema
}

func (a NotifySchemaDecorator) SchemaInitializingQueries(params wm_sql.SchemaInitializingQueriesParams) ([]wm_sql.Query, error) {
	queries, err := a.DefaultPostgreSQLSchema.SchemaInitializingQueries(params)
	if err != nil {
		return nil, err
	}

	messageTable := a.DefaultPostgreSQLSchema.MessagesTable(params.Topic)

	// Create a single shared notification function (idempotent)
	queries = append(queries,
		wm_sql.Query{
			Query: `CREATE OR REPLACE FUNCTION watermill_notify_message() RETURNS TRIGGER AS $$
		BEGIN
			PERFORM pg_notify('watermill_messages', TG_ARGV[0]);
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;`,
		},
		wm_sql.Query{
			Query: fmt.Sprintf(`DROP TRIGGER IF EXISTS watermill_notify_trigger ON %[1]s;
		CREATE TRIGGER watermill_notify_trigger
		AFTER INSERT ON %[1]s
		FOR EACH STATEMENT
		EXECUTE FUNCTION watermill_notify_message('%[2]s');`, messageTable, params.Topic),
		})

	return queries, nil
}
