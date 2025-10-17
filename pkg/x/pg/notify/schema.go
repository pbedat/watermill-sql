package notify

import (
	"fmt"

	wm_sql "github.com/ThreeDotsLabs/watermill-sql/v4/pkg/sql"
)

type TriggerOffsetsAdapter struct {
	wm_sql.DefaultPostgreSQLOffsetsAdapter
}

func (a TriggerOffsetsAdapter) BeforeSubscribingQueries(params wm_sql.BeforeSubscribingQueriesParams) ([]wm_sql.Query, error) {
	queries, err := a.DefaultPostgreSQLOffsetsAdapter.BeforeSubscribingQueries(params)
	if err != nil {
		return nil, err
	}

	messageTable := fmt.Sprintf(`"watermill_%s"`, params.Topic)

	queries = append(queries,
		wm_sql.Query{
			Query: fmt.Sprintf(`CREATE OR REPLACE FUNCTION notify_new_message_%[1]s() RETURNS TRIGGER AS $$
		BEGIN
			NOTIFY watermill_new_messages_%[1]s;
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;`, params.Topic),
		},
		wm_sql.Query{
			Query: fmt.Sprintf(`DROP TRIGGER IF EXISTS new_message_trigger_%[1]s ON %[2]s;
		CREATE TRIGGER new_message_trigger_%[1]s
		AFTER INSERT ON %[2]s
		FOR EACH STATEMENT
		EXECUTE FUNCTION notify_new_message_%[1]s();`, params.Topic, messageTable),
		})

	return queries, nil
}
