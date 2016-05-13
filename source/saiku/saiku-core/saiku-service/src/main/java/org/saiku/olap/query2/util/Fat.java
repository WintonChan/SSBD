package org.saiku.olap.query2.util;

import org.saiku.olap.query2.*;
import org.saiku.olap.query2.ThinMeasure.Type;
import org.saiku.olap.query2.ThinQueryModel.AxisLocation;
import org.saiku.olap.query2.common.ThinQuerySet;
import org.saiku.olap.query2.common.ThinSortableQuerySet;
import org.saiku.olap.query2.filter.ThinFilter;
import org.saiku.query.*;
import org.saiku.query.QueryDetails.Location;
import org.saiku.query.mdx.*;
import org.saiku.query.mdx.IFilterFunction.MdxFunctionType;
import org.saiku.query.metadata.CalculatedMeasure;
import org.saiku.query.metadata.CalculatedMember;

import org.apache.commons.lang.StringUtils;
import org.olap4j.Axis;
import org.olap4j.OlapException;
import org.olap4j.metadata.*;

import java.sql.SQLException;
import java.util.*;

public class Fat {
	
	public static Query convert(ThinQuery tq, Cube cube) throws SQLException {
		
		Query q = new Query(tq.getName(), cube);
		if (tq.getParameters() != null) {
			q.setParameters(tq.getParameters());
		}
		
		if (tq.getQueryModel() == null)
			return q;

		ThinQueryModel model = tq.getQueryModel();
		convertAxes(q, tq.getQueryModel().getAxes(), tq);
		convertCalculatedMeasures(q, model.getCalculatedMeasures());
	  	convertCalculatedMembers(q, model.getCalculatedMembers());
		convertDetails(q, model.getDetails());
		q.setVisualTotals(model.isVisualTotals());
		q.setVisualTotalsPattern(model.getVisualTotalsPattern());
		return q;
	}

  private static void convertCalculatedMembers(Query q, List<ThinCalculatedMember> thinCms) {
	/*Hierarchy h = q.getCube().getHierarchies().get("Products");
	CalculatedMember cm =
		new CalculatedMember(
			h.getDimension(),
			h,
			"Consumable",
			"Consumable",
			null,
			Member.Type.FORMULA,
			"Aggregate({Product.Drink, Product.Food})",
			null);
	q.addCalculatedMember(q.getHierarchy(h), cm);

	try {
	  q.getHierarchy(h).includeCalculatedMember(cm);
	} catch (OlapException e) {
	  e.printStackTrace();
	}*/
	/*if (thinCms != null && thinCms.size() > 0) {
	  for (ThinCalculatedMember qcm : thinCms) {
		NamedList<Hierarchy> h2 = q.getCube().getHierarchies();
		for(Hierarchy h: h2){
		  if(h.getUniqueName().equals(qcm.getHierarchyName())){
			CalculatedMember cm =
				new CalculatedMember(
					h.getDimension(),
					h,
					qcm.getName(),
					qcm.getName(),
					null,
					Member.Type.FORMULA,
					qcm.getFormula(),
					null);
			q.addCalculatedMember(q.getHierarchy(h), cm);
			break;
		  }
		}

	  }
	}*/
  }

  private static void convertCalculatedMeasures(Query q, List<ThinCalculatedMeasure> thinCms) {
		if (thinCms != null && thinCms.size() > 0) {
			for (ThinCalculatedMeasure qcm : thinCms) {
			  Hierarchy h = q.getCube().getHierarchies().get("Measures");
			  CalculatedMeasure cm =
					  new CalculatedMeasure(
						  h,
						  qcm.getName(),
						  null,
						  qcm.getFormula(),
						  qcm.getProperties());

				  q.addCalculatedMeasure(cm);

			}
		}
	}

	private static void convertDetails(Query query, ThinDetails details) {
		Location loc = Location.valueOf(details.getLocation().toString());
		query.getDetails().setLocation(loc);
		Axis ax = getLocation(details.getAxis());
		query.getDetails().setAxis(ax);
		
		if (details != null && details.getMeasures().size() > 0) {
			for (ThinMeasure m : details.getMeasures()) {
				if (Type.CALCULATED.equals(m.getType())) {
					Measure measure = query.getCalculatedMeasure(m.getName());
					query.getDetails().add(measure);
				} else if (Type.EXACT.equals(m.getType())) {
					Measure measure = query.getMeasure(m.getName());
					query.getDetails().add(measure);
				}
			}
		}
	}

	private static void convertAxes(Query q, Map<AxisLocation, ThinAxis> axes, ThinQuery tq) throws OlapException {
		if (axes != null) {
			for (AxisLocation axis : sortAxes(axes.keySet())) {
				if (axis != null) {
					convertAxis(q, axes.get(axis), tq);
				}
			}
		}
	}
	
	private static List<AxisLocation> sortAxes(Set<AxisLocation> axes) {
		List<AxisLocation> ax = new ArrayList<>();
		for (AxisLocation a : AxisLocation.values()) {
			if (axes.contains(a)){
				ax.add(a);
			}
		}
		return ax;
	}
	
	

	private static void convertAxis(Query query, ThinAxis thinAxis, ThinQuery tq) throws OlapException {
		Axis loc = getLocation(thinAxis.getLocation());
		QueryAxis qaxis = query.getAxis(loc);
		for (ThinHierarchy hierarchy : thinAxis.getHierarchies()) {
			QueryHierarchy qh = query.getHierarchy(hierarchy.getName());
			if (qh != null) {
				convertHierarchy(query, qh, hierarchy, tq);
				qaxis.addHierarchy(qh);
			}
		}
		qaxis.setNonEmpty(thinAxis.isNonEmpty());
		List<String> aggs = thinAxis.getAggregators();
		qaxis.getQuery().setAggregators(qaxis.getLocation().toString(), aggs);
		extendSortableQuerySet(query, qaxis, thinAxis);
	}
	
	private static void convertHierarchy(Query q, QueryHierarchy qh, ThinHierarchy th, ThinQuery tq) throws
		OlapException {
	  for (Object o : th.getCmembers().entrySet()) {
		Map.Entry pair = (Map.Entry) o;

		ThinCalculatedMember cres = null;
		for (ThinCalculatedMember c : tq.getQueryModel().getCalculatedMembers()) {
		  if (c.getUniqueName().equals(pair.getValue())) {
			cres = c;
			break;
		  }
		  //it.remove(); // avoids a ConcurrentModificationException
		}
		Hierarchy h2 = null;
		for (Hierarchy h : q.getCube().getHierarchies()) {
		  if (h.getUniqueName().equals(cres.getHierarchyName())) {
			h2 = h;
			break;
		  }
		}
		CalculatedMember cm;
		cm = new CalculatedMember(
			q.getCube().getDimensions().get(cres.getDimension()),
			h2,
			cres.getName(),
			cres.getName(),
			null,
			Member.Type.FORMULA,
			cres.getFormula(),
			null);

		qh.includeCalculatedMember(cm);
		extendSortableQuerySet(qh.getQuery(), qh, th);

	  }

		for (ThinLevel tl : th.getLevels().values()) {
		  QueryLevel ql = qh.includeLevel(tl.getName());


		  if (ql == null) {
			qh.includeMember(th.getName() + ".[" + tl.getName() + "]");
		  } else {
			List<String> aggs = tl.getAggregators();
			qh.getQuery().setAggregators(ql.getUniqueName(), aggs);

			if (tl.getSelection() != null) {
			  String parameter = tl.getSelection().getParameterName();
			  if (StringUtils.isNotBlank(parameter)) {
				ql.setParameterName(parameter);
				ql.setParameterSelectionType(org.saiku.query.Parameter.SelectionType.INCLUSION);
			  }
			  switch (tl.getSelection().getType()) {
			  case INCLUSION:
//					if (parameterValues != null) {
//						for (String m : parameterValues) {
//							qh.includeMember(m);
//						}
//
//					} else {
				for (ThinMember tm : tl.getSelection().getMembers()) {
				  qh.includeMember(tm.getUniqueName());
				}
				ql.setParameterSelectionType(org.saiku.query.Parameter.SelectionType.INCLUSION);
//					}
				break;

			  case EXCLUSION:
//					if (parameterValues != null) {
//						for (String m : parameterValues) {
//							qh.excludeMember(m);
//						}
//
//					} else {
				for (ThinMember tm : tl.getSelection().getMembers()) {
				  qh.excludeMember(tm.getUniqueName());
				}
				ql.setParameterSelectionType(org.saiku.query.Parameter.SelectionType.EXCLUSION);
//					}
				break;
			  case RANGE:
				int size = tl.getSelection().getMembers().size();
				int iterations = tl.getSelection().getMembers().size() / 2;
				if (size > 2 && size % 2 == 0) {
				  for (int i = 0; i < iterations; i++) {
					ThinMember start = tl.getSelection().getMembers().get(iterations * 2 + i);
					ThinMember end = tl.getSelection().getMembers().get(iterations * 2 + i + 1);
					qh.includeRange(start.getUniqueName(), end.getUniqueName());
				  }
				}
				break;
			  default:
				break;

			  }
			}

			extendQuerySet(qh.getQuery(), ql, tl);
		  }
		  extendSortableQuerySet(qh.getQuery(), qh, th);
		}

	}


	private static Axis getLocation(AxisLocation axis) {
		String ax = axis.toString();
		if (AxisLocation.ROWS.toString().equals(ax)) {
			return Axis.ROWS;
		} else if (AxisLocation.COLUMNS.toString().equals(ax)) {
			return Axis.COLUMNS;
		} else if (AxisLocation.FILTER.toString().equals(ax)) {
			return Axis.FILTER;
		} else if (AxisLocation.PAGES.toString().equals(ax)) {
			return Axis.PAGES;
		}
		return null;
	}

	private static void extendQuerySet(Query q, IQuerySet qs, ThinQuerySet ts) {
		qs.setMdxSetExpression(ts.getMdx());
		
		if (ts.getFilters() != null && ts.getFilters().size() > 0) {
			List<IFilterFunction> filters = convertFilters(q, ts.getFilters());
			qs.getFilters().addAll(filters);
		}
		
	}
	
	private static List<IFilterFunction> convertFilters(Query q, List<ThinFilter> filters) {
		List<IFilterFunction> qfs = new ArrayList<>();
		for (ThinFilter f : filters) {
			switch(f.getFlavour()) {
				case Name:
					List<String> exp = f.getExpressions();
					if (exp != null && exp.size() > 1) {
						String hierarchyName = exp.remove(0);
						QueryHierarchy qh = q.getHierarchy(hierarchyName);
						NameFilter nf = new NameFilter(qh.getHierarchy(), exp);
						qfs.add(nf);
					}
					break;
				case NameLike:
					List<String> exp2 = f.getExpressions();
					if (exp2 != null && exp2.size() > 1) {
						String hierarchyName = exp2.remove(0);
						QueryHierarchy qh = q.getHierarchy(hierarchyName);
						NameLikeFilter nf = new NameLikeFilter(qh.getHierarchy(), exp2);
						qfs.add(nf);
					}
					break;
				case Generic:
					List<String> gexp = f.getExpressions();
					if (gexp != null && gexp.size() == 1) {
						GenericFilter gf = new GenericFilter(gexp.get(0));
						qfs.add(gf);
					}
					break;
				case Measure:
					// TODO Implement this
					break;
				case N:
					List<String> nexp = f.getExpressions();
					if (nexp != null && nexp.size() > 0) {
						MdxFunctionType mf = MdxFunctionType.valueOf(f.getFunction().toString());
						int n = Integer.parseInt(nexp.get(0));
						String expression = null;
						if (nexp.size() > 1) {
							expression = nexp.get(1);
						}
						NFilter nf = new NFilter(mf, n, expression);
						qfs.add(nf);
					}
					break;
				default:
					break;
			}
		}
		return qfs;
	}

	private static void extendSortableQuerySet(Query q, ISortableQuerySet qs, ThinSortableQuerySet ts) {
		extendQuerySet(q, qs, ts);
		if (ts.getHierarchizeMode() != null) {
			qs.setHierarchizeMode(org.saiku.query.ISortableQuerySet.HierarchizeMode.valueOf(ts.getHierarchizeMode().toString()));
		}
		if (ts.getSortOrder() != null) {
			qs.sort(org.saiku.query.SortOrder.valueOf(ts.getSortOrder().toString()), ts.getSortEvaluationLiteral());
		}
		
		
	}
	

}
