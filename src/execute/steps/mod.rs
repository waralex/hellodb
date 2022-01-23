pub mod processor;
use std::cell::RefCell;
use std::rc::Rc;
use crate::DBResult;
use crate::blocks::{BlockRef, ColumnBlock};


pub enum ProcessStatus
{
    MustStop,
    MustGoOn
}

pub trait Processor
{
    fn run(&mut self, input :BlockRef, output :BlockRef) -> DBResult<ProcessStatus>;
    fn finalize(&mut self, output :BlockRef) -> DBResult<()>;
}

pub type ProcessorRef = Box<dyn Processor>;

pub struct ExecuteStep
{
    input :BlockRef,
    output :BlockRef,
    processors :Vec<ProcessorRef>,
}

impl ExecuteStep
{
    pub fn new(input: ColumnBlock, output: ColumnBlock) -> Self
    {
        Self {
            input :Rc::new(RefCell::new(input)),
            output :Rc::new(RefCell::new(output)),
            processors: Vec::<ProcessorRef>::new()
        }
    }

    pub fn add_proc(&mut self, proc:ProcessorRef) -> &mut Self
    {
        self.processors.push(proc);
        self
    }

    pub fn output(&self) -> BlockRef
    {
        self.output.clone()
    }


    pub fn execute(&mut self) -> DBResult<()>
    {
        let mut stopped = false;
        while !stopped {
            for p in self.processors.iter_mut()
            {
                match p.run(self.input.clone(), self.output.clone())? {
                    ProcessStatus::MustStop => {stopped = true},
                    ProcessStatus::MustGoOn => {}
                }
            }
        }
        for p in self.processors.iter_mut()
        {
            p.finalize(self.output.clone())?;
        }
        Ok(())
    }
}