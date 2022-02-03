use super::*;
use crate::functions::aggregate::*;

struct AggrItem
{
    aggr_col :AggrColumnRef,
    srcs :Vec<usize>,
    dest :usize
}

pub struct AggrProcessor
{
    group_inds :Vec<usize>,
    items :Vec<AggrItem>
}

impl AggrProcessor
{
    pub fn new(group_inds:Vec<usize>) -> Self
    {
        Self{group_inds, items:Vec::new()}
    }

    pub fn add_item(&mut self, aggr_col:AggrColumnRef, srcs:Vec<usize>, dest:usize)
    {
        self.items.push(AggrItem{aggr_col, srcs, dest})
    }
}

