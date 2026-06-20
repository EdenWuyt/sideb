using Core.Interfaces;
using Core.Entities;

namespace Infrastructure.Data;

public class SpecificationEvaluator<T> where T : BaseEntity
{
    public static IQueryable<T> GetQuery(IQueryable<T> inputQuery, ISpecification<T> spec)
    {
        var query = inputQuery;
        
        if (spec.Criteria != null)
        {
            query = query.Where(spec.Criteria); // apply where clause from specification
        }

        if (spec.OrderBy != null)
        {
            query = query.OrderBy(spec.OrderBy); // apply order by clause from specification
        }

        if (spec.OrderByDescending != null)
        {
            query = query.OrderByDescending(spec.OrderByDescending); // apply order by descending clause from specification
        }

        if (spec.IsDistinct)
        {
            query = query.Distinct(); // apply distinct clause from specification
        }

        if (spec.IsPagingEnabled)
        {
            query = query.Skip(spec.Skip).Take(spec.Take); // apply pagination from specification
        }

        return query;
    }

    public static IQueryable<TResult> GetQuery<TResult>(IQueryable<T> inputQuery, ISpecification<T, TResult> spec)
    {
        var query = GetQuery(inputQuery, spec as ISpecification<T>); // apply criteria and ordering from specification

        if (spec.Criteria != null)
        {
            query = query.Where(spec.Criteria); // apply where clause from specification
        }

        if (spec.OrderBy != null)
        {
            query = query.OrderBy(spec.OrderBy); // apply order by clause from specification
        }

        if (spec.OrderByDescending != null)
        {
            query = query.OrderByDescending(spec.OrderByDescending); // apply order by descending clause from specification
        }

        if (spec.Select == null)
        {
            throw new InvalidOperationException("Select expression is required.");
        }

        var selectedQuery = query.Select(spec.Select); // apply select clause from specification

        if (spec.IsDistinct)
        {
            selectedQuery = selectedQuery.Distinct(); // apply distinct clause from specification
        }

        if (spec.IsPagingEnabled)
        {
            selectedQuery = selectedQuery.Skip(spec.Skip).Take(spec.Take); // apply pagination from specification
        }

        return selectedQuery;
    }
}