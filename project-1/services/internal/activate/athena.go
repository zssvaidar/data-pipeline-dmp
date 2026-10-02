package activate

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/athena"
	"github.com/aws/aws-sdk-go-v2/service/athena/types"
)

// Athena runs queries in a workgroup (which sets the result location and
// the bytes-scanned limit) and waits for them to finish.
type Athena struct {
	Client    *athena.Client
	WorkGroup string
	Poll      time.Duration
}

func (a *Athena) Query(ctx context.Context, sql string, params []string) ([][]string, error) {
	start, err := a.Client.StartQueryExecution(ctx, &athena.StartQueryExecutionInput{
		QueryString:         aws.String(sql),
		WorkGroup:           aws.String(a.WorkGroup),
		ExecutionParameters: params,
	})
	if err != nil {
		return nil, err
	}
	id := start.QueryExecutionId

	poll := a.Poll
	if poll == 0 {
		poll = time.Second
	}
	for {
		out, err := a.Client.GetQueryExecution(ctx, &athena.GetQueryExecutionInput{QueryExecutionId: id})
		if err != nil {
			return nil, err
		}
		status := out.QueryExecution.Status
		switch status.State {
		case types.QueryExecutionStateSucceeded:
			return a.results(ctx, id)
		case types.QueryExecutionStateFailed, types.QueryExecutionStateCancelled:
			return nil, fmt.Errorf("query %s %s: %s", aws.ToString(id), status.State, aws.ToString(status.StateChangeReason))
		}
		select {
		case <-ctx.Done():
			_, _ = a.Client.StopQueryExecution(context.WithoutCancel(ctx), &athena.StopQueryExecutionInput{QueryExecutionId: id})
			return nil, ctx.Err()
		case <-time.After(poll):
		}
	}
}

func (a *Athena) results(ctx context.Context, id *string) ([][]string, error) {
	var rows [][]string
	p := athena.NewGetQueryResultsPaginator(a.Client, &athena.GetQueryResultsInput{QueryExecutionId: id})
	first := true
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, r := range page.ResultSet.Rows {
			if first { // the first row of the first page is the header
				first = false
				continue
			}
			row := make([]string, len(r.Data))
			for i, d := range r.Data {
				row[i] = aws.ToString(d.VarCharValue)
			}
			rows = append(rows, row)
		}
	}
	return rows, nil
}
